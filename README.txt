Pyramid RESTful Framework is designed to help coding REST CRUD endpoints with couple of lines of code.

Setup.

First, lets install pyramid and create an app.

1. virtualenv myapp
2. pip install pyramid
3. pcreate -s starter myapp
4. pip install -e .

Now if we run pserve development.ini and navigate to http://localhost:6543 we will see the standard pyramid app. Boring.

Lets add prf to the mix!

pip install git+https://github.com/vahana/prf

And add resources.

In the __init__.main function of your pyramid app declare your resources:

def main():
  ...
  config.include('prf')
  root = config.get_root_resource()
  user = root.add('user', view='prf.view.NoOp')
  user_story = user.add('story', 'stories', view='prf.view.NoOp')
  ...
  
The following endpoints are declared with the code above:
/users/{id}
/users/{user_id}/stories/{id}

You can now navigate to http://0.0.0.0:6543/users or http://0.0.0.0:6543/users/1/stories

'NoOp' view as name suggests does not do much. You will need to create your own views for each resource.
In our case UsersView and UserStoriesView.

UsersView could look something like this:

from prf.view import BaseView

Users = [
  {
    'id': 0,
    'name':'Alice',
  },
  {
    'id': 1,
    'name':'Bob',
  },
  {
    'id': 2,
    'name':'Katy',
  },
]

class UsersView(BaseView):

  def index(self):
     return Users

  def show(self, id):
     return Users[int(id)]

  def create(self):
     Users.update(**self._params)

  def delete(self, id):
     del Users[id]

You need to change the view for the users resource to point to this class:
user = root.add('user', view=UsersView)

Restart the server and navigate to http://0.0.0.0:6543/users

Above we declared `index`, `show`, `create` and `delete` actions which correspond to: GET collection, GET resource, POST resource and DELETE resource respectively. You could also declare `update`, which would correspond to the PUT method. You dont need to declare all of them, only those you need. The missing ones will automatically return 405 Method Not Allowed error.

Comment out the `index` action and try.

Happy RESTing !

