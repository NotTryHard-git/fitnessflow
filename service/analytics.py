import asyncio
import asyncpg
import grpc
import json
import aiokafka
from datetime import datetime
import logging
import time
import analytics_pb2
import analytics_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('AnalyticsService')

class AnalyticsService(analytics_pb2_grpc.AnalyticsServiceServicer):
    
    def __init__(self):
        self.pg_conn = None
        self.kafka_consumer = None
        self.unique_users_cache = {}
        
    async def pg_connect(self):
        for _ in range(5):
            try:
                self.pg_conn = await asyncpg.connect(
                    host='postgres-analytics',
                    database='analytics_db',
                    user='postgres',
                    password='postgres',
                    port=5432
                )
                logger.info("Successfully connected to analytics database")
                await self.setup_tables()
                break
            except Exception as e:
                logger.error(f"Database connection error, attempt {_}: {e}")
                await asyncio.sleep(5)
    async def setup_tables(self):
        """Создание таблиц в PostgreSQL"""
        try:           
            # Таблица месячной статистики
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS monthly_statistics (
                    id SERIAL PRIMARY KEY,
                    month_year VARCHAR(7) NOT NULL,
                    total_bookings INTEGER DEFAULT 0,
                    unique_users INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(month_year)
                )
            """)
            
            # Вставляем тестовые данные
            await self.pg_conn.execute("""
                INSERT INTO monthly_statistics (month_year, total_bookings, unique_users) VALUES
                (TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYY-MM'), 42, 28),
                (TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYY-MM'), 58, 35),
                (TO_CHAR(CURRENT_DATE, 'YYYY-MM'), 12, 8)
                ON CONFLICT (month_year) DO NOTHING
            """)
            
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    async def kafka_connect(self):
        try:
            self.kafka_consumer = aiokafka.AIOKafkaConsumer(
                'booking-events',
                bootstrap_servers='kafka:9092',
                group_id='analytics-service',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            await self.kafka_consumer.start()
            logger.info("Successfully connected to Kafka")
            
            # Start consuming messages in background
            asyncio.create_task(self.consume_messages())
            
        except Exception as e:
            logger.error(f"Kafka connection error: {e}")
    
    async def consume_messages(self):
        """Consume messages from Kafka and update statistics"""
        try:
            async for message in self.kafka_consumer:
                event = message.value
                logger.info(f"Received event: {event}")
                
                if event['event_type'] == 'BookingConfirmed':
                    await self.update_statistics(event, increment=True)
                elif event['event_type'] == 'BookingCancelled':
                    await self.update_statistics(event, increment=False)
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
    
    async def update_statistics(self, event, increment=True):
        """Update monthly statistics based on event"""
        try:
            event_time = datetime.fromisoformat(event['timestamp'])
            month_year = event_time.strftime('%Y-%m')
            user_id = event['user_id']
            
            # Check if this is first booking for this user in this month
            is_unique = False
            if month_year not in self.unique_users_cache:
                self.unique_users_cache[month_year] = set()
            
            if user_id not in self.unique_users_cache[month_year]:
                is_unique = True
                self.unique_users_cache[month_year].add(user_id)
            
            # Prepare update
            total_change = 1 if increment else -1
            unique_change = 1 if is_unique and increment else 0
            
            # Update database
            await self.pg_conn.execute(
                """
                INSERT INTO monthly_statistics (month_year, total_bookings, unique_users)
                VALUES ($1, $2, $3)
                ON CONFLICT (month_year) DO UPDATE 
                SET total_bookings = monthly_statistics.total_bookings + $2,
                    unique_users = monthly_statistics.unique_users + $3,
                    updated_at = CURRENT_TIMESTAMP
                """,
                month_year,
                total_change,
                unique_change
            )
            
            logger.info(f"Updated statistics for {month_year}: total_change={total_change}, unique_change={unique_change}")
            
        except Exception as e:
            logger.error(f"Failed to update statistics: {e}")
    
    async def UpdateStatistics(self, request, context):
        """Manual statistics update"""
        try:
            await self.pg_conn.execute(
                """
                INSERT INTO monthly_statistics (month_year, total_bookings, unique_users)
                VALUES ($1, $2, $3)
                ON CONFLICT (month_year) DO UPDATE 
                SET total_bookings = monthly_statistics.total_bookings + $2,
                    unique_users = monthly_statistics.unique_users + $3,
                    updated_at = CURRENT_TIMESTAMP
                """,
                request.month_year,
                request.total_bookings_change,
                request.unique_users_change
            )
            
            return analytics_pb2.StatisticsResponse(
                success=True,
                message='Statistics updated successfully'
            )
        except Exception as e:
            logger.error(f'UpdateStatistics failed: {str(e)}')
            return analytics_pb2.StatisticsResponse(
                success=False,
                message=f'UpdateStatistics failed: {str(e)}'
            )
    
    async def GetMonthlyStatistics(self, request, context):
        try:
            query = "SELECT * FROM monthly_statistics"
            params = []
            
            if request.month_year:
                query += " WHERE month_year = $1"
                params.append(request.month_year)
            
            query += " ORDER BY month_year DESC"
            
            stats = await self.pg_conn.fetch(query, *params)
            
            response = analytics_pb2.MonthlyStatisticsResponse()
            for stat in stats:
                response.statistics.append(analytics_pb2.MonthlyStatistic(
                    month_year=str(stat['month_year']),
                    total_bookings=int(stat['total_bookings']),
                    unique_users=int(stat['unique_users']),
                    updated_at=str(stat['updated_at'])
                ))
            
            return response
        except Exception as e:
            logger.error(f'GetMonthlyStatistics failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetMonthlyStatistics failed: {str(e)}')
            return analytics_pb2.MonthlyStatisticsResponse()

async def serve():
    server = grpc.aio.server()
    service = AnalyticsService()
    await service.pg_connect()
    time.sleep(30)
    await service.kafka_connect()
    analytics_pb2_grpc.add_AnalyticsServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50056')
    
    await server.start()
    logger.info("Analytics Service started on port 50056")
    
    await server.wait_for_termination()

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Analytics Service stopped by user")
    except Exception as e:
        logger.error(f"Analytics Service crashed: {str(e)}")