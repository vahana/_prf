Pyramid RESTful Framework is designed to help coding REST CRUD endpoints with couple of lines of code.

Setup.

1. virtualenv myapp
2. pip install git+https://github.com/vahana/prf
3. pcreate -s starter myapp
4. cd myapp
5. pip install -e .

Run.

pserve development.ini

This will run a server and you can navigate your browser to http://0.0.0.0:6543


Adding prf resources.

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

