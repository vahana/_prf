Pyramid RESTful Framework is designed to help coding REST CRUD endpoints with couple of lines of code.

In the "main" function of your pyramid app declare your resources:

def main():
  ...
  config.include('prf')
  root = config.get_root_resource()
  user = root.add('user', view=views.UsersView) 
  user_story = user.add('story', 'stories', view='views.UserStoriesView') 
  ...
  
The following endpoints are declared with the code above:
/users/{id}
/users/{user_id}/stories/{id}

Corresponding views would look something like:

from prf.view import BaseView
from model import User

class UsersView(BaseView):

def index(self):
   return User.query.all()

def show(self, id):
   return User.get(id)

def create(self):
   return User(**self._params).save()

def delete(self, id):
   User.get(id).delete()

Defined actions are: index (GET), show (GET), create(POST), update(PUT/PATCH), delete(DELETE).
If its not defined in your view, prf will return HTTPMethodNotAllowed by default. 
View can return either objects, list of objects, http exception or request object. 
Framework will convert all the returned results into Respone objects with ContentType='application/json' by default and serialize the body into json.
