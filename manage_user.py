from flask import request, jsonify, make_response
import uuid # for public id
from  werkzeug.security import generate_password_hash, check_password_hash
# imports for PyJWT authentication
import jwt
from datetime import datetime, timedelta
from functools import wraps  
from appSetup import app
import configparser
from user_defined_class import *

config_obj = configparser.ConfigParser()
config_obj.read("config.ini")
JwtParam = config_obj["jwt"]

  
# decorator for verifying the JWT
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        print('-----------')
        print(request.headers)
        print('-----------')

        # jwt is passed in the request header
        if 'X-Access-Token' in request.headers:
            token = request.headers['X-Access-Token']
        # return 401 if token is not passed
        if not token:
            return jsonify({'message' : 'Token is missing !!'}), 401
  
        # try:
            # decoding the payload to fetch the stored details
        data = jwt.decode(token, JwtParam['secretKey'], algorithms=['HS256'])
        # print({'data: ', data});
        current_user = User.objects(email=data['email']).first()
        # except:
        #     return jsonify({
        #         'message' : 'Token is invalid !!'
        #     }), 401
        # returns the current logged in users contex to the routes
        return  f(current_user, *args, **kwargs)
    return decorated
  
# User Database Route
# this route sends back list of users users
@app.route('/user', methods =['GET'])
@token_required
def get_all_users(current_user):
    # querying the database
    # for all the entries in it
    users = User.objects()
    # converting the query objects
    # to list of jsons
    output = []
    for user in users:
        # appending the user data json
        # to the response list
        output.append({
            'role': user['role'],
            'name' : user['name'],
            'email' : user['email']
        })
  
    return jsonify({'users': output})
  
# route for logging user in
@app.route('/login', methods =['POST'])
def login():

    jsonData = request.get_json()

    print(jsonData)
  
    if not jsonData or not jsonData['email'] or not jsonData['password']:
        # returns 401 if any email or / and password is missing
        return make_response(
            'Could not verify',
            401,
            {
                'WWW-Authenticate' : 'Basic realm ="Login required !!"'
            }
        )
  
    user = User.objects(email = jsonData['email']).first()
  
    if not user:
        # returns 401 if user does not exist
        return make_response(
            'Could not verify',
            401,
            {'WWW-Authenticate' : 'Basic realm ="User does not exist !!"'}
        )
  
    if check_password_hash(user['password'], jsonData['password']):
        # generates the JWT Token
        token = jwt.encode({
            'name': user['name'],
            'email': user['email'],
            'role': user['role'],
            'exp' : datetime.utcnow() + timedelta(minutes = 120)
        }, JwtParam['secretKey'])
  
        return make_response(jsonify({'token' : token}), 201)
    # returns 403 if password is wrong
    return make_response(
        'Could not verify',
        403,
        {'WWW-Authenticate' : 'Basic realm ="Wrong Password !!"'}
    )
  
# signup route
@app.route('/signup', methods =['POST'])
def signup():
    # creates a dictionary of the form data
    jsonData = request.get_json()
    print('------')
    print(jsonData)
    print('------')
  
    # gets name, email and password
    name, email, role = jsonData['name'], jsonData['email'], jsonData['role']
    password = jsonData['password']
  
    # checking for existing user
    user = User.objects(email = email).first()

    if not user:
        # database ORM object
        user = User(
            name = name,
            email = email,
            role = role,
            password = generate_password_hash(password)
        )
        # insert user
        user.save()
        
        return jsonify({'body': user})
    else:
        return make_response('User already exists. Please Log in.', 400)
