from flask import Flask, request, jsonify
import grpc
import asyncio
import json

import user_pb2
import user_pb2_grpc
import schedule_pb2
import schedule_pb2_grpc
import booking_pb2
import booking_pb2_grpc
import attendance_pb2
import attendance_pb2_grpc
import analytics_pb2
import analytics_pb2_grpc

from functools import wraps
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Gateway')

app = Flask(__name__)

SERVICE_CONFIG = {
    'user': 'user-service:50052',
    'schedule': 'schedule-service:50051',
    'booking': 'booking-service:50053',
    'attendance': 'attendance-service:50054',
    'analytics': 'analytics-service:50055'
}

# ========== User Service Client ==========
class UserClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['user']
    
    async def CreateUser(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.CreateUser(user_pb2.CreateUserRequest(
                    email=data['email'],
                    first_name=data['first_name'],
                    last_name=data['last_name'],
                    phone=data.get('phone', ''),
                    user_type=data.get('user_type', 'client'),
                    subscription_end=data.get('subscription_end', ''),
                    password=data.get('password', '')
                ))
            return {
                'user_id': response.user_id,
                'email': response.email,
                'first_name': response.first_name,
                'last_name': response.last_name,
                'phone': response.phone,
                'user_type': response.user_type,
                'subscription_end': response.subscription_end,
                'status': response.status
            }
        except Exception as e:
            logger.error(f"CreateUser error: {e}")
            return {'error': str(e)}
    
    async def GetUser(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.GetUser(user_pb2.GetUserRequest(
                    user_id=data['user_id']
                ))
            return {
                'user_id': response.user_id,
                'email': response.email,
                'first_name': response.first_name,
                'last_name': response.last_name,
                'phone': response.phone,
                'user_type': response.user_type,
                'subscription_end': response.subscription_end,
                'status': response.status
            }
        except grpc.RpcError as e:
            logger.error(f"GetUser error: {e.details()}")
            return {'error': e.details()}
    
    async def Authenticate(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.Authenticate(user_pb2.AuthRequest(
                    email=data['email'],
                    password=data['password']
                ))
            return {
                'success': response.success,
                'token': response.token if response.token else '',
                'user_id': response.user_id if response.user_id else '',
                'user_type': response.user_type if response.user_type else ''
            }
        except Exception as e:
            logger.error(f"Authenticate error: {e}")
            return {'error': str(e)}
    
    async def ValidateUser(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.ValidateUser(user_pb2.ValidateUserRequest(
                    user_id=data['user_id'],
                    workout_datetime=data.get('workout_datetime', '')
                ))
            return {
                'valid': response.valid,
                'message': response.message
            }
        except Exception as e:
            logger.error(f"ValidateUser error: {e}")
            return {'error': str(e)}

# ========== Schedule Service Client ==========
class ScheduleClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['schedule']
    
    async def CreateWorkout(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.CreateWorkout(schedule_pb2.CreateWorkoutRequest(
                    name=data['name'],
                    trainer_id=data['trainer_id'],
                    room_id=data['room_id'],
                    datetime=data['datetime'],
                    max_participants=data['max_participants'],
                    description=data.get('description', '')
                ))
            return {
                'workout_id': response.workout_id,
                'name': response.name,
                'trainer_id': response.trainer_id,
                'room_id': response.room_id,
                'datetime': response.datetime,
                'max_participants': response.max_participants,
                'current_participants': response.current_participants,
                'description': response.description,
                'status': response.status
            }
        except Exception as e:
            logger.error(f"CreateWorkout error: {e}")
            return {'error': str(e)}
    
    async def GetWorkout(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.GetWorkout(schedule_pb2.GetWorkoutRequest(
                    workout_id=data['workout_id']
                ))
            return {
                'workout_id': response.workout_id,
                'name': response.name,
                'trainer_id': response.trainer_id,
                'room_id': response.room_id,
                'datetime': response.datetime,
                'max_participants': response.max_participants,
                'current_participants': response.current_participants,
                'description': response.description,
                'status': response.status
            }
        except grpc.RpcError as e:
            logger.error(f"GetWorkout error: {e.details()}")
            return {'error': e.details()}
    
    async def ListWorkouts(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.ListWorkouts(schedule_pb2.ListWorkoutsRequest(
                    date_from=data.get('date_from', ''),
                    date_to=data.get('date_to', ''),
                    trainer_id=data.get('trainer_id', ''),
                    room_id=data.get('room_id', '')
                ))
            
            workouts = []
            for workout in response.workouts:
                workouts.append({
                    'workout_id': workout.workout_id,
                    'name': workout.name,
                    'trainer_id': workout.trainer_id,
                    'room_id': workout.room_id,
                    'datetime': workout.datetime,
                    'max_participants': workout.max_participants,
                    'current_participants': workout.current_participants,
                    'description': workout.description,
                    'status': workout.status
                })
            
            return {'workouts': workouts}
        except Exception as e:
            logger.error(f"ListWorkouts error: {e}")
            return {'error': str(e)}
    
    async def CheckAvailability(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.CheckAvailability(schedule_pb2.CheckAvailabilityRequest(
                    workout_id=data['workout_id']
                ))
            return {
                'available': response.available,
                'available_slots': response.available_slots
            }
        except Exception as e:
            logger.error(f"CheckAvailability error: {e}")
            return {'error': str(e)}

# ========== Booking Service Client ==========
class BookingClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['booking']
    
    async def CreateBooking(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = booking_pb2_grpc.BookingServiceStub(channel)
                response = await stub.CreateBooking(booking_pb2.CreateBookingRequest(
                    user_id=data['user_id'],
                    workout_id=data['workout_id']
                ))
            return {
                'booking_id': response.booking_id,
                'user_id': response.user_id,
                'workout_id': response.workout_id,
                'status': response.status,
                'created_at': response.created_at
            }
        except Exception as e:
            logger.error(f"CreateBooking error: {e}")
            return {'error': str(e)}
    
    async def CancelBooking(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = booking_pb2_grpc.BookingServiceStub(channel)
                response = await stub.CancelBooking(booking_pb2.CancelBookingRequest(
                    booking_id=data['booking_id'],
                    reason=data.get('reason', 'User cancelled')
                ))
            return {
                'success': response.success,
                'message': response.message
            }
        except Exception as e:
            logger.error(f"CancelBooking error: {e}")
            return {'error': str(e)}
    
    async def GetBooking(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = booking_pb2_grpc.BookingServiceStub(channel)
                response = await stub.GetBooking(booking_pb2.GetBookingRequest(
                    booking_id=data['booking_id']
                ))
            return {
                'booking_id': response.booking_id,
                'user_id': response.user_id,
                'workout_id': response.workout_id,
                'status': response.status,
                'created_at': response.created_at,
                'updated_at': response.updated_at,
                'reason': response.reason
            }
        except grpc.RpcError as e:
            logger.error(f"GetBooking error: {e.details()}")
            return {'error': e.details()}
    
    async def GetUserBookings(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = booking_pb2_grpc.BookingServiceStub(channel)
                response = await stub.GetUserBookings(booking_pb2.GetUserBookingsRequest(
                    user_id=data['user_id'],
                    status=data.get('status', '')
                ))
            
            bookings = []
            for booking in response.bookings:
                bookings.append({
                    'booking_id': booking.booking_id,
                    'user_id': booking.user_id,
                    'workout_id': booking.workout_id,
                    'status': booking.status,
                    'created_at': booking.created_at,
                    'updated_at': booking.updated_at,
                    'reason': booking.reason
                })
            
            return {'bookings': bookings}
        except Exception as e:
            logger.error(f"GetUserBookings error: {e}")
            return {'error': str(e)}

# ========== Attendance Service Client ==========
class AttendanceClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['attendance']
    
    async def CreateNotification(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = attendance_pb2_grpc.AttendanceServiceStub(channel)
                response = await stub.CreateNotification(attendance_pb2.CreateNotificationRequest(
                    user_id=data['user_id'],
                    type=data['type'],
                    title=data['title'],
                    message=data['message'],
                    related_booking_id=data.get('related_booking_id', ''),
                    related_workout_id=data.get('related_workout_id', '')
                ))
            return {
                'notification_id': response.notification_id,
                'user_id': response.user_id,
                'type': response.type,
                'title': response.title,
                'message': response.message,
                'created_at': response.created_at,
                'related_booking_id': response.related_booking_id if response.related_booking_id else '',
                'related_workout_id': response.related_workout_id if response.related_workout_id else ''
            }
        except Exception as e:
            logger.error(f"CreateNotification error: {e}")
            return {'error': str(e)}
    
    async def GetUserNotifications(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = attendance_pb2_grpc.AttendanceServiceStub(channel)
                response = await stub.GetUserNotifications(attendance_pb2.GetUserNotificationsRequest(
                    user_id=data['user_id'],
                    limit=data.get('limit', 0)
                ))
            
            notifications = []
            for notification in response.notifications:
                notifications.append({
                    'notification_id': notification.notification_id,
                    'user_id': notification.user_id,
                    'type': notification.type,
                    'title': notification.title,
                    'message': notification.message,
                    'created_at': notification.created_at,
                    'related_booking_id': notification.related_booking_id,
                    'related_workout_id': notification.related_workout_id
                })
            
            return {'notifications': notifications}
        except Exception as e:
            logger.error(f"GetUserNotifications error: {e}")
            return {'error': str(e)}
    
    async def MarkAttendance(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = attendance_pb2_grpc.AttendanceServiceStub(channel)
                response = await stub.MarkAttendance(attendance_pb2.MarkAttendanceRequest(
                    user_id=data['user_id'],
                    workout_id=data['workout_id'],
                    attended=data['attended']
                ))
            return {
                'success': response.success,
                'message': response.message
            }
        except Exception as e:
            logger.error(f"MarkAttendance error: {e}")
            return {'error': str(e)}
    
    async def GetAttendance(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = attendance_pb2_grpc.AttendanceServiceStub(channel)
                response = await stub.GetAttendance(attendance_pb2.GetAttendanceRequest(
                    workout_id=data['workout_id']
                ))
            
            attendance_records = []
            for record in response.attendance:
                attendance_records.append({
                    'user_id': record.user_id,
                    'workout_id': record.workout_id,
                    'attended': record.attended,
                    'checked_in_at': record.checked_in_at
                })
            
            return {
                'attendance': attendance_records,
                'total': response.total,
                'attended': response.attended
            }
        except Exception as e:
            logger.error(f"GetAttendance error: {e}")
            return {'error': str(e)}

# ========== Analytics Service Client ==========
class AnalyticsClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['analytics']
    
    async def RecordBooking(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = analytics_pb2_grpc.AnalyticsServiceStub(channel)
                response = await stub.RecordBooking(analytics_pb2.RecordBookingRequest(
                    user_id=data['user_id'],
                    workout_id=data.get('workout_id', ''),
                    booking_date=data['booking_date']
                ))
            return {
                'success': response.success,
                'message': response.message
            }
        except Exception as e:
            logger.error(f"RecordBooking error: {e}")
            return {'error': str(e)}
    
    async def GetMonthlyStatistics(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = analytics_pb2_grpc.AnalyticsServiceStub(channel)
                response = await stub.GetMonthlyStatistics(analytics_pb2.GetMonthlyStatisticsRequest(
                    month_year=data.get('month_year', '')
                ))
            return {
                'month_year': response.month_year,
                'total_bookings': response.total_bookings,
                'unique_users': response.unique_users,
                'booking_growth': response.booking_growth
            }
        except Exception as e:
            logger.error(f"GetMonthlyStatistics error: {e}")
            return {'error': str(e)}
    
    async def GetWorkoutStatistics(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = analytics_pb2_grpc.AnalyticsServiceStub(channel)
                response = await stub.GetWorkoutStatistics(analytics_pb2.GetWorkoutStatisticsRequest(
                    workout_id=data['workout_id']
                ))
            return {
                'workout_id': response.workout_id,
                'workout_name': response.workout_name,
                'total_bookings': response.total_bookings,
                'attendance_rate': response.attendance_rate,
                'avg_participants': response.avg_participants
            }
        except Exception as e:
            logger.error(f"GetWorkoutStatistics error: {e}")
            return {'error': str(e)}

# ========== Initialize Clients ==========
user_client = UserClient()
schedule_client = ScheduleClient()
booking_client = BookingClient()
attendance_client = AttendanceClient()
analytics_client = AnalyticsClient()

# ========== Authentication Middleware ==========
def get_current_user():
    """Получение текущего пользователя из заголовков"""
    try:
        token = request.headers.get('Authorization', '')
        if token.startswith('Bearer '):
            token = token[7:]
        
        if not token:
            return None
            
        # В реальном приложении здесь была бы проверка токена
        # Для демо возвращаем ID из заголовка
        user_id = request.headers.get('X-User-ID', '')
        user_type = request.headers.get('X-User-Type', 'client')
        
        return {
            'user_id': user_id,
            'user_type': user_type,
            'token': token
        }
    except Exception as e:
        logger.error(f"get_current_user error: {e}")
        return None

def require_auth(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        return await func(*args, **kwargs)
    return wrapper

def require_admin(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        
        if user.get('user_type') != 'admin':
            return jsonify({'error': 'Admin privileges required'}), 403
        
        return await func(*args, **kwargs)
    return wrapper

def require_trainer(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Authentication required'}), 401
        
        user_type = user.get('user_type', '')
        if user_type not in ['admin', 'trainer']:
            return jsonify({'error': 'Trainer or admin privileges required'}), 403
        
        return await func(*args, **kwargs)
    return wrapper

# ========== User Routes ==========
@app.route('/api/users/signup', methods=['POST'])
async def signup():
    data = request.get_json()
    result = await user_client.CreateUser(data)
    return jsonify(result)

@app.route('/api/users/signin', methods=['POST'])
async def signin():
    data = request.get_json()
    result = await user_client.Authenticate(data)
    return jsonify(result)

@app.route('/api/users/me', methods=['GET'])
@require_auth
async def get_current_user_info():
    user = get_current_user()
    if not user or not user.get('user_id'):
        return jsonify({'error': 'User not found'}), 404
    
    result = await user_client.GetUser({'user_id': user['user_id']})
    return jsonify(result)

@app.route('/api/users/<user_id>', methods=['GET'])
@require_admin
async def get_user(user_id):
    result = await user_client.GetUser({'user_id': user_id})
    return jsonify(result)

@app.route('/api/users/validate', methods=['POST'])
@require_auth
async def validate_user():
    data = request.get_json()
    user = get_current_user()
    if user and user.get('user_id'):
        data['user_id'] = user['user_id']
    
    result = await user_client.ValidateUser(data)
    return jsonify(result)

# ========== Schedule Routes ==========
@app.route('/api/workouts', methods=['POST'])
@require_trainer
async def create_workout():
    data = request.get_json()
    result = await schedule_client.CreateWorkout(data)
    return jsonify(result)

@app.route('/api/workouts', methods=['GET'])
@require_auth
async def list_workouts():
    data = {
        'date_from': request.args.get('date_from', ''),
        'date_to': request.args.get('date_to', ''),
        'trainer_id': request.args.get('trainer_id', ''),
        'room_id': request.args.get('room_id', '')
    }
    result = await schedule_client.ListWorkouts(data)
    return jsonify(result)

@app.route('/api/workouts/<workout_id>', methods=['GET'])
@require_auth
async def get_workout(workout_id):
    result = await schedule_client.GetWorkout({'workout_id': workout_id})
    return jsonify(result)

@app.route('/api/workouts/<workout_id>/availability', methods=['GET'])
@require_auth
async def check_availability(workout_id):
    result = await schedule_client.CheckAvailability({'workout_id': workout_id})
    return jsonify(result)

# ========== Booking Routes ==========
@app.route('/api/bookings', methods=['POST'])
@require_auth
async def create_booking():
    data = request.get_json()
    user = get_current_user()
    
    # Проверяем пользователя
    validation = await user_client.ValidateUser({
        'user_id': user['user_id'],
        'workout_datetime': data.get('workout_datetime', '')
    })
    
    if 'error' in validation:
        return jsonify(validation), 400
    
    if not validation.get('valid', False):
        return jsonify({'error': 'User validation failed', 'details': validation.get('message', '')}), 400
    
    # Проверяем доступность тренировки
    availability = await schedule_client.CheckAvailability({
        'workout_id': data['workout_id']
    })
    
    if 'error' in availability:
        return jsonify(availability), 400
    
    if not availability.get('available', False):
        return jsonify({'error': 'No available slots'}), 400
    
    # Создаем бронирование
    data['user_id'] = user['user_id']
    result = await booking_client.CreateBooking(data)
    
    if 'error' not in result and result.get('booking_id'):
        # Записываем в аналитику
        await analytics_client.RecordBooking({
            'user_id': user['user_id'],
            'workout_id': data['workout_id'],
            'booking_date': datetime.now().isoformat()
        })
        
        # Создаем уведомление
        await attendance_client.CreateNotification({
            'user_id': user['user_id'],
            'type': 'BOOKING_CREATED',
            'title': 'Booking Created',
            'message': 'Your booking has been created and is being processed',
            'related_booking_id': result['booking_id'],
            'related_workout_id': data['workout_id']
        })
    
    return jsonify(result)

@app.route('/api/bookings', methods=['GET'])
@require_auth
async def get_user_bookings():
    user = get_current_user()
    status = request.args.get('status', '')
    
    result = await booking_client.GetUserBookings({
        'user_id': user['user_id'],
        'status': status
    })
    return jsonify(result)

@app.route('/api/bookings/<booking_id>', methods=['GET'])
@require_auth
async def get_booking(booking_id):
    result = await booking_client.GetBooking({'booking_id': booking_id})
    return jsonify(result)

@app.route('/api/bookings/<booking_id>/cancel', methods=['POST'])
@require_auth
async def cancel_booking(booking_id):
    data = request.get_json()
    data['booking_id'] = booking_id
    
    # Проверяем, принадлежит ли бронирование пользователю
    booking = await booking_client.GetBooking({'booking_id': booking_id})
    user = get_current_user()
    
    if 'error' in booking:
        return jsonify(booking), 400
    
    if booking.get('user_id') != user.get('user_id'):
        # Проверяем, является ли пользователь админом или тренером
        if user.get('user_type') not in ['admin', 'trainer']:
            return jsonify({'error': 'Not authorized to cancel this booking'}), 403
    
    result = await booking_client.CancelBooking(data)
    
    if 'error' not in result and result.get('success'):
        # Освобождаем слот
        await schedule_client.ReleaseSlot({
            'workout_id': booking.get('workout_id'),
            'slots': 1
        })
        
        # Создаем уведомление
        await attendance_client.CreateNotification({
            'user_id': booking.get('user_id'),
            'type': 'BOOKING_CANCELLED',
            'title': 'Booking Cancelled',
            'message': f'Your booking has been cancelled. Reason: {data.get("reason", "User cancelled")}',
            'related_booking_id': booking_id,
            'related_workout_id': booking.get('workout_id')
        })
    
    return jsonify(result)

# ========== Attendance Routes ==========
@app.route('/api/notifications', methods=['GET'])
@require_auth
async def get_user_notifications():
    user = get_current_user()
    limit = request.args.get('limit', 0, type=int)
    
    result = await attendance_client.GetUserNotifications({
        'user_id': user['user_id'],
        'limit': limit
    })
    return jsonify(result)

@app.route('/api/attendance', methods=['POST'])
@require_trainer
async def mark_attendance():
    data = request.get_json()
    result = await attendance_client.MarkAttendance(data)
    return jsonify(result)

@app.route('/api/workouts/<workout_id>/attendance', methods=['GET'])
@require_trainer
async def get_workout_attendance(workout_id):
    result = await attendance_client.GetAttendance({'workout_id': workout_id})
    return jsonify(result)

# ========== Analytics Routes ==========
@app.route('/api/analytics/monthly', methods=['GET'])
@require_admin
async def get_monthly_statistics():
    month_year = request.args.get('month_year', '')
    result = await analytics_client.GetMonthlyStatistics({'month_year': month_year})
    return jsonify(result)

@app.route('/api/analytics/workouts/<workout_id>', methods=['GET'])
@require_trainer
async def get_workout_statistics(workout_id):
    result = await analytics_client.GetWorkoutStatistics({'workout_id': workout_id})
    return jsonify(result)

# ========== Health Check ==========
@app.route('/api/health', methods=['GET'])
async def health_check():
    services = ['user', 'schedule', 'booking', 'attendance', 'analytics']
    health_status = {}
    
    for service in services:
        try:
            if service == 'user':
                await user_client.Authenticate({'email': 'test@test.com', 'password': 'test'})
            elif service == 'schedule':
                await schedule_client.ListWorkouts({})
            elif service == 'booking':
                await booking_client.GetBooking({'booking_id': 'test'})
            elif service == 'attendance':
                await attendance_client.GetUserNotifications({'user_id': 'test', 'limit': 1})
            elif service == 'analytics':
                await analytics_client.GetMonthlyStatistics({})
            
            health_status[service] = 'healthy'
        except Exception as e:
            logger.error(f"Health check failed for {service}: {e}")
            health_status[service] = 'unhealthy'
    
    all_healthy = all(status == 'healthy' for status in health_status.values())
    return jsonify({
        'status': 'healthy' if all_healthy else 'degraded',
        'services': health_status
    }), 200 if all_healthy else 503

# ========== Error Handlers ==========
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500



# ========== Main ==========
if __name__ == '__main__':
    logger.info("Starting API Gateway...")
    app.run(host='0.0.0.0', port=8080, debug=True)