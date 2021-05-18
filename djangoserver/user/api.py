from rest_framework.response import Response
from rest_framework.decorators import api_view
from .models import User
from .modeldto import UserSerializer


@api_view(['GET'])
def UserIdCreate(request):
    serialiser = request.d
    return Response()