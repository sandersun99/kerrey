import os
import pika
import json
import logging
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, EmailStr
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# --- 1. 配置日志记录 ---
# 配置日志格式，包括时间、日志级别和消息内容
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. 初始化请求限制器 ---
# 根据请求的来源IP进行限制
limiter = Limiter(key_func=get_remote_address)

# --- 3. 创建 FastAPI 应用实例 ---
app = FastAPI()

# --- 4. 将限制器应用到 app ---
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# --- 5. 定义经过严格验证的数据模型 ---
class Task(BaseModel):
    """定义传入任务的数据结构和验证规则"""
    keyword: str = Field(
        ...,
        description="搜索关键词",
        max_length=100  # 限制最大长度为100
    )
    email: EmailStr  # 自动验证必须是合法的邮箱格式

# --- 6. 定义API接口 ---
@app.get("/")
def read_root():
    """根路径，用于简单的服务健康检查"""
    logging.info("服务健康检查被调用")
    return {"status": "API Server is running"}

@app.post("/submit_task")
@limiter.limit("10/minute")  # 同一个IP每分钟最多提交10次
def submit_task(request: Request, task: Task):
    """
    接收并处理客户端提交的任务。
    包含输入验证、请求限制和专业的日志记录。
    """
    amqp_url = os.environ.get('CLOUDAMQP_URL')
    if not amqp_url:
        logging.error("服务器环境变量 CLOUDAMQP_URL 未设置！")
        raise HTTPException(status_code=500, detail="服务器配置错误")

    try:
        task_data = task.model_dump_json() # 使用 pydantic v2 的方法
        logging.info(f"收到来自 {request.client.host} 的有效任务: {task_data}")

        # --- 连接到 CloudAMQP 并发送消息 ---
        params = pika.URLParameters(amqp_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue='tasks', durable=True)
        channel.basic_publish(
            exchange='',
            routing_key='tasks',
            body=task_data,
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
        )
        connection.close()
        
        logging.info(f"任务已成功推送到队列: {task_data}")
        return {"message": "任务提交成功"}

    except pika.exceptions.AMQPError as e:
        # 捕获具体的消息队列错误
        logging.error(f"消息队列(Pika)发生错误: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail="消息队列服务异常")
    except Exception as e:
        # 捕获其他所有未知错误
        logging.error(f"处理任务时发生未知错误: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="服务器内部未知错误")