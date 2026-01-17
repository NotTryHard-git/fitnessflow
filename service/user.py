import asyncio
import asyncpg
import grpc
from datetime import datetime
import logging

import user_pb2
import user_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('UserService')

class UserService(user_pb2_grpc.UserServiceServicer):
    
    def __init__(self):
        self.pg_conn = None
        
    async def pg_connect(self):
        for _ in range(5):
            try:
                self.pg_conn = await asyncpg.connect(
                    host='postgres-user',
                    database='user_db',
                    user='postgres',
                    password='postgres',
                    port=5432
                )
                logger.info("Successfully connected to user database")
                await self.setup_tables()
                break
            except Exception as e:
                logger.error(f"Database connection error, attempt {_}: {e}")
                await asyncio.sleep(5)
    async def setup_tables(self):
        """Создание таблиц в PostgreSQL"""
        try:
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS user_type (
                    type_id SERIAL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL UNIQUE
                )
            """)
            
            await self.pg_conn.execute("""
                INSERT INTO user_type (type_id, name) 
                VALUES 
                    (1, 'client'),
                    (2, 'trainer'),
                    (3, 'admin')
                ON CONFLICT (type_id) DO NOTHING
            """)
            
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id VARCHAR(36) PRIMARY KEY,
                    password VARCHAR(36),
                    email VARCHAR(255) NOT NULL UNIQUE,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    phone VARCHAR(20),
                    type_id INTEGER REFERENCES user_type(type_id) DEFAULT 1,
                    subscription_end DATE,
                    status VARCHAR(20) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    async def CreateUser(self, request, context):
        try:
            user_id = f'user-{datetime.now().strftime("%Y%m%d%H%M%S")}'
            
            # Преобразуем строку даты в объект date, если она передана
            subscription_end = None
            if request.subscription_end:
                try:
                    subscription_end = datetime.strptime(request.subscription_end, '%Y-%m-%d').date()
                except ValueError:
                    subscription_end = None
            
            await self.pg_conn.execute(
                """
                INSERT INTO users 
                (user_id, email, password, first_name, last_name, phone, type_id, subscription_end)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                user_id,
                request.email,
                request.password or 'default123',
                request.first_name,
                request.last_name,
                request.phone,
                request.type_id,
                subscription_end  # Теперь это объект date или None
            )
            
            return await self.GetUser(
                user_pb2.GetUserRequest(user_id=user_id),
                context
            )
        except Exception as e:
            logger.error(f'CreateUser failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'CreateUser failed: {str(e)}')
            return user_pb2.UserResponse()
    async def GetUser(self, request, context):
        try:
            user = await self.pg_conn.fetchrow(
                "SELECT * FROM users WHERE user_id = $1",
                request.user_id
            )
            
            if not user:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('User not found')
                return user_pb2.UserResponse()
            
            return user_pb2.UserResponse(
                user_id=str(user['user_id']),
                email=str(user['email']),
                first_name=str(user['first_name']),
                last_name=str(user['last_name']),
                phone=str(user['phone']),
                type_id=int(user['type_id']),
                subscription_end=str(user['subscription_end']),
                created_at=str(user['created_at']),
                updated_at=str(user['updated_at']),
                status=str(user['status'])
            )
        except Exception as e:
            logger.error(f'GetUser failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetUser failed: {str(e)}')
            return user_pb2.UserResponse()
    async def ValidateSubscription(self, request, context):
        try:
            user = await self.pg_conn.fetchrow(
                "SELECT subscription_end, status FROM users WHERE user_id = $1",
                request.user_id
            )
            
            if not user:
                return user_pb2.ValidationResponse(
                    valid=False,
                    message='User not found'
                )
            
            # Check if user is active
            if user['status'] != 'active':
                return user_pb2.ValidationResponse(
                    valid=False,
                    message='User account is inactive'
                )
            
            # Check subscription (only for clients - type_id 1)
            user_type = await self.pg_conn.fetchval(
                "SELECT type_id FROM users WHERE user_id = $1",
                request.user_id
            )
            
            if user_type == 1:  # Client
                if not user['subscription_end']:
                    return user_pb2.ValidationResponse(
                        valid=False,
                        message='No active subscription'
                    )
                
                subscription_end = user['subscription_end']
                if subscription_end < datetime.now().date():
                    return user_pb2.ValidationResponse(
                        valid=False,
                        message='Subscription has expired'
                    )
            
            return user_pb2.ValidationResponse(
                valid=True,
                message='Subscription is valid'
            )
            
        except Exception as e:
            logger.error(f'ValidateSubscription failed: {str(e)}')
            return user_pb2.ValidationResponse(
                valid=False,
                message=f'Validation failed: {str(e)}'
            )
    
    async def CreateUser(self, request, context):
        try:
            user_id = f'user-{datetime.now().strftime("%Y%m%d%H%M%S")}'
            
            # Преобразуем строку даты в объект date, если она передана
            subscription_end = None
            if request.subscription_end:
                try:
                    subscription_end = datetime.strptime(request.subscription_end, '%Y-%m-%d').date()
                except ValueError:
                    subscription_end = None
            
            await self.pg_conn.execute(
                """
                INSERT INTO users 
                (user_id, email, password, first_name, last_name, phone, type_id, subscription_end)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                user_id,
                request.email,
                request.password or 'default123',
                request.first_name,
                request.last_name,
                request.phone,
                request.type_id,
                subscription_end  # Теперь это объект date или None
            )
            
            return await self.GetUser(
                user_pb2.GetUserRequest(user_id=user_id),
                context
            )
        except Exception as e:
            logger.error(f'CreateUser failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'CreateUser failed: {str(e)}')
            return user_pb2.UserResponse()
    
    async def GetUsersByType(self, request, context):
        try:
            users = await self.pg_conn.fetch(
                "SELECT * FROM users WHERE type_id = $1 ORDER BY created_at DESC",
                request.type_id
            )
            
            response = user_pb2.UsersResponse()
            for user in users:
                response.users.append(user_pb2.UserResponse(
                    user_id=str(user['user_id']),
                    email=str(user['email']),
                    first_name=str(user['first_name']),
                    last_name=str(user['last_name']),
                    phone=str(user['phone']),
                    type_id=int(user['type_id']),
                    subscription_end=str(user['subscription_end']),
                    created_at=str(user['created_at']),
                    updated_at=str(user['updated_at']),
                    status=str(user['status'])
                ))
            
            return response
        except Exception as e:
            logger.error(f'GetUsersByType failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetUsersByType failed: {str(e)}')
            return user_pb2.UsersResponse()

async def serve():
    server = grpc.aio.server()
    service = UserService()
    await service.pg_connect()
    user_pb2_grpc.add_UserServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50052')
    
    await server.start()
    logger.info("User Service started on port 50052")
    
    await server.wait_for_termination()

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("User Service stopped by user")
    except Exception as e:
        logger.error(f"User Service crashed: {str(e)}")