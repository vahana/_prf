Pyramid RESTful Framework (PRF) is designed to help coding RESTful endpoints with minimal code. It takes care of lots of reasonable defaults and boilerplate code.

Setup.

First, lets install pyramid and create an app:

```
virtualenv myapp
pip install pyramid
pcreate -s starter myapp
pip install -e .
```

Now if we run 
```
pserve development.ini
``` 

and navigate to http://localhost:6543 we will see the standard pyramid app. Boring.

Lets install `httpie` to use it for doing requests to our endpoints. Feel free to use curl or any other http client as long as it supports CRUDs.

And lets add prf to the mix!

```
pip install git+https://github.com/vahana/prf
```

And add resources.

Modify `__init__.main` function of myapp to look like:

```
def main(global_config, **settings):
    config = Configurator(settings=settings)

    config.include('prf') #pyramid way of adding external packages.
    root = config.get_root_resource() #acquire root resource.
    user = root.add('user', 'users', view='prf.view.NoOp') # declare `users` root resource
    user_story = user.add('story', 'stories', view='prf.view.NoOp') # declare `nested resource `users/stories`

    #per pyramid, must return wsgi app
    return config.make_wsgi_app()
 ```
 
The following endpoints are declared with the code above:

```
users/{id}
users/{user_id}/stories/{id}
```

Try these:

```
# will get all declared resources
http 0.0.0.0:6543/_
# will get users
http 0.0.0.0:6543/users
will get stories for a user with id 1
http 0.0.0.0:6543/users/1/stories
```

'NoOp' view, as name suggests, does not do much. We will need to create our own views for each resource.
In our case UsersView and UserStoriesView.

Lets modify views.py to add the following:

```
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
    Users.pop(int(id))

```

We need to change the view argument for the `users` resource to point to our new class in the `main`:
```
user = root.add('user', view='myapp.views.UsersView')
```

Restart the server and try:
```
# list users
http 0.0.0.0:6543/users
# delete a user with id 1
http DELETE 0.0.0.0:6543/users/1
# user 1 is gone
http 0.0.0.0:6543/users
```

Above, we declared `index`, `show`, `create` and `delete` actions which correspond to: GET collection, GET resource, POST resource and DELETE resource respectively. You could also declare `update`, which would correspond to the PUT method. You dont need to declare all of them, only those you need. The missing ones will automatically return 405 Method Not Allowed error.

Comment out the `index` action and try:
```
http 0.0.0.0:6543/users
```

Happy RESTing !

