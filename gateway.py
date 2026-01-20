from flask import Flask, request, jsonify
import grpc
import asyncio
from functools import wraps

import schedule_pb2, schedule_pb2_grpc
import booking_pb2, booking_pb2_grpc
import user_pb2, user_pb2_grpc
import notification_pb2, notification_pb2_grpc
import analytics_pb2, analytics_pb2_grpc

app = Flask(__name__)

SERVICE_CONFIG = {
    'schedule': 'schedule-service:50051',
    'booking': 'booking-service:50053',
    'user': 'user-service:50052',
    'notification': 'notification-service:50055',
    'analytics': 'analytics-service:50056'
}

class ScheduleClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['schedule']
    
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
            return {'error': e.details()}
    
    async def GetWorkouts(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.GetWorkouts(schedule_pb2.GetWorkoutsRequest(
                    date_from=data.get('date_from', ''),
                    date_to=data.get('date_to', '')
                ))
            return {
                'workouts': [{
                    'workout_id': w.workout_id,
                    'name': w.name,
                    'trainer_id': w.trainer_id,
                    'room_id': w.room_id,
                    'datetime': w.datetime,
                    'max_participants': w.max_participants,
                    'current_participants': w.current_participants,
                    'description': w.description,
                    'status': w.status
                } for w in response.workouts]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
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
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def ReserveSlot(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.ReserveSlot(schedule_pb2.ReserveSlotRequest(
                    workout_id=data['workout_id']
                ))
            return {
                'success': response.success,
                'message': response.message,
                'current_participants': response.current_participants
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    async def CancelReservation(self, data):
        """Добавленный метод для отмены резервирования"""
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.CancelReservation(schedule_pb2.CancelReservationRequest(
                    workout_id=data['workout_id']
                ))
            return {
                'success': response.success,
                'message': response.message
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    async def GetTrainers(self):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.GetTrainers(schedule_pb2.Empty())
            return {
                'trainers': [{
                    'trainer_id': t.trainer_id,
                    'name': t.name,
                    'specialty': t.specialty,
                    'email': t.email,
                    'phone': t.phone
                } for t in response.trainers]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def GetRooms(self):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                response = await stub.GetRooms(schedule_pb2.Empty())
            return {
                'rooms': [{
                    'room_id': r.room_id,
                    'name': r.name,
                    'capacity': r.capacity
                } for r in response.rooms]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

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
                'created_at': response.created_at,
                'updated_at': response.updated_at
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def CancelBooking(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = booking_pb2_grpc.BookingServiceStub(channel)
                response = await stub.CancelBooking(booking_pb2.CancelBookingRequest(
                    booking_id=data['booking_id']
                ))
            return {
                'booking_id': response.booking_id,
                'user_id': response.user_id,
                'workout_id': response.workout_id,
                'status': response.status,
                'created_at': response.created_at,
                'updated_at': response.updated_at
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def GetUserBookings(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = booking_pb2_grpc.BookingServiceStub(channel)
                response = await stub.GetUserBookings(booking_pb2.GetUserBookingsRequest(
                    user_id=data['user_id']
                ))
            return {
                'bookings': [{
                    'booking_id': b.booking_id,
                    'user_id': b.user_id,
                    'workout_id': b.workout_id,
                    'status': b.status,
                    'created_at': b.created_at,
                    'updated_at': b.updated_at
                } for b in response.bookings]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
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
                'updated_at': response.updated_at
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

class UserClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['user']
    
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
                'type_id': response.type_id,
                'subscription_end': response.subscription_end,
                'created_at': response.created_at,
                'updated_at': response.updated_at,
                'status': response.status
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def CreateUser(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.CreateUser(user_pb2.CreateUserRequest(
                    email=data['email'],
                    password=data['password'],
                    first_name=data['first_name'],
                    last_name=data['last_name'],
                    phone=data.get('phone', ''),
                    type_id=data.get('type_id', 1),
                    subscription_end=data.get('subscription_end', '')
                ))
            return {
                'user_id': response.user_id,
                'email': response.email,
                'first_name': response.first_name,
                'last_name': response.last_name,
                'phone': response.phone,
                'type_id': response.type_id,
                'subscription_end': response.subscription_end,
                'created_at': response.created_at,
                'updated_at': response.updated_at,
                'status': response.status
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def ValidateSubscription(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.ValidateSubscription(user_pb2.ValidateSubscriptionRequest(
                    user_id=data['user_id']
                ))
            return {
                'valid': response.valid,
                'message': response.message
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def GetUsersByType(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.GetUsersByType(user_pb2.GetUsersByTypeRequest(
                    type_id=data['type_id']
                ))
            return {
                'users': [{
                    'user_id': u.user_id,
                    'email': u.email,
                    'first_name': u.first_name,
                    'last_name': u.last_name,
                    'phone': u.phone,
                    'type_id': u.type_id,
                    'subscription_end': u.subscription_end,
                    'created_at': u.created_at,
                    'updated_at': u.updated_at,
                    'status': u.status
                } for u in response.users]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

class NotificationClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['notification']
    
    async def GetUserNotifications(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = notification_pb2_grpc.NotificationServiceStub(channel)
                response = await stub.GetUserNotifications(notification_pb2.GetUserNotificationsRequest(
                    user_id=data['user_id'],
                    limit=data.get('limit', 10)
                ))
            return {
                'notifications': [{
                    'notification_id': n.notification_id,
                    'user_id': n.user_id,
                    'type': n.type,
                    'title': n.title,
                    'message': n.message,
                    'created_at': n.created_at,
                    'related_booking_id': n.related_booking_id,
                    'related_workout_id': n.related_workout_id
                } for n in response.notifications]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def SendNotification(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = notification_pb2_grpc.NotificationServiceStub(channel)
                response = await stub.SendNotification(notification_pb2.SendNotificationRequest(
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
                'related_booking_id': response.related_booking_id,
                'related_workout_id': response.related_workout_id
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

class AnalyticsClient:
    def __init__(self):
        self.service_address = SERVICE_CONFIG['analytics']
    
    async def GetMonthlyStatistics(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = analytics_pb2_grpc.AnalyticsServiceStub(channel)
                response = await stub.GetMonthlyStatistics(analytics_pb2.GetMonthlyStatisticsRequest(
                    month_year=data.get('month_year', '')
                ))
            return {
                'statistics': [{
                    'month_year': s.month_year,
                    'total_bookings': s.total_bookings,
                    'unique_users': s.unique_users,
                    'updated_at': s.updated_at
                } for s in response.statistics]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    async def UpdateStatistics(self, data):
        try:
            async with grpc.aio.insecure_channel(self.service_address) as channel:
                stub = analytics_pb2_grpc.AnalyticsServiceStub(channel)
                response = await stub.UpdateStatistics(analytics_pb2.UpdateStatisticsRequest(
                    month_year=data['month_year'],
                    total_bookings_change=data['total_bookings_change'],
                    unique_users_change=data['unique_users_change']
                ))
            return {
                'success': response.success,
                'message': response.message
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

# Initialize clients
schedule_client = ScheduleClient()
booking_client = BookingClient()
user_client = UserClient()
notification_client = NotificationClient()
analytics_client = AnalyticsClient()

# Authentication middleware (simplified)
def require_auth(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # In real implementation, validate JWT token
        return await func(*args, **kwargs)
    return wrapper

def require_admin(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # In real implementation, check admin role
        return await func(*args, **kwargs)
    return wrapper

# Schedule endpoints
@app.route('/api/schedule/workouts', methods=['GET'])
@require_auth
async def get_workouts():
    data = request.args.to_dict()
    result = await schedule_client.GetWorkouts(data)
    return jsonify(result)

@app.route('/api/schedule/workouts', methods=['POST'])
@require_admin
async def create_workout():
    data = request.get_json()
    result = await schedule_client.CreateWorkout(data)
    return jsonify(result)

@app.route('/api/schedule/workouts/<workout_id>', methods=['GET'])
@require_auth
async def get_workout(workout_id):
    result = await schedule_client.GetWorkout({'workout_id': workout_id})
    return jsonify(result)

@app.route('/api/schedule/trainers', methods=['GET'])
@require_auth
async def get_trainers():
    result = await schedule_client.GetTrainers()
    return jsonify(result)

@app.route('/api/schedule/rooms', methods=['GET'])
@require_auth
async def get_rooms():
    result = await schedule_client.GetRooms()
    return jsonify(result)

# Booking endpoints
@app.route('/api/bookings', methods=['POST'])
@require_auth
async def create_booking():
    data = request.get_json()
    result = await booking_client.CreateBooking(data)
    return jsonify(result)

@app.route('/api/bookings/<booking_id>', methods=['DELETE'])
@require_auth
async def cancel_booking(booking_id):
    result = await booking_client.CancelBooking({'booking_id': booking_id})
    return jsonify(result)

@app.route('/api/bookings/user/<user_id>', methods=['GET'])
@require_auth
async def get_user_bookings(user_id):
    result = await booking_client.GetUserBookings({'user_id': user_id})
    return jsonify(result)

@app.route('/api/bookings/<booking_id>', methods=['GET'])
@require_auth
async def get_booking(booking_id):
    result = await booking_client.GetBooking({'booking_id': booking_id})
    return jsonify(result)

# User endpoints
@app.route('/api/users', methods=['POST'])
@require_admin
async def create_user():
    data = request.get_json()
    result = await user_client.CreateUser(data)
    return jsonify(result)

@app.route('/api/users/<user_id>', methods=['GET'])
@require_auth
async def get_user(user_id):
    result = await user_client.GetUser({'user_id': user_id})
    return jsonify(result)

@app.route('/api/users/validate/<user_id>', methods=['GET'])
@require_auth
async def validate_user_subscription(user_id):
    result = await user_client.ValidateSubscription({'user_id': user_id})
    return jsonify(result)

@app.route('/api/users/type/<type_id>', methods=['GET'])
@require_admin
async def get_users_by_type(type_id):
    result = await user_client.GetUsersByType({'type_id': int(type_id)})
    return jsonify(result)

# Notification endpoints
@app.route('/api/notifications/user/<user_id>', methods=['GET'])
@require_auth
async def get_user_notifications(user_id):
    data = {'user_id': user_id, 'limit': request.args.get('limit', 10, type=int)}
    result = await notification_client.GetUserNotifications(data)
    return jsonify(result)

# Analytics endpoints
@app.route('/api/analytics/monthly', methods=['GET'])
@require_admin
async def get_monthly_statistics():
    data = {'month_year': request.args.get('month_year', '')}
    result = await analytics_client.GetMonthlyStatistics(data)
    return jsonify(result)

@app.route('/api/health', methods=['GET'])
async def health_check():
    return jsonify({'status': 'healthy'})

@app.route('/api/schedule/workouts/<workout_id>/reserve', methods=['POST'])
@require_auth
async def reserve_slot(workout_id):
    result = await schedule_client.ReserveSlot({'workout_id': workout_id})
    return jsonify(result)

@app.route('/api/schedule/workouts/<workout_id>/cancel', methods=['POST'])
@require_auth
async def cancel_reservation(workout_id):
    result = await schedule_client.CancelReservation({'workout_id': workout_id})
    return jsonify(result)

@app.route('/api/notifications', methods=['POST'])
@require_auth
async def send_notification():
    data = request.get_json()
    result = await notification_client.SendNotification(data)
    return jsonify(result)

@app.route('/api/analytics/update', methods=['POST'])
@require_admin
async def update_statistics():
    data = request.get_json()
    result = await analytics_client.UpdateStatistics(data)
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)