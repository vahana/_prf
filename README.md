Pyramid RESTful Framework is designed to help coding REST CRUD endpoints with couple of lines of code.

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

Lets add prf to the mix!

```
pip install git+https://github.com/vahana/prf
```

And add resources.

Modify `__init__.main` function of myapp to look like:

```
def main(global_config, **settings):
    config = Configurator(settings=settings)

    config.include('prf')
    root = config.get_root_resource()
    user = root.add('user', view='prf.view.NoOp')
    user_story = user.add('story', 'stories', view='prf.view.NoOp')

    config.scan()
    return config.make_wsgi_app()
 ```
 
We removed the generated code, acquired a root resource using `config.get_root_resource` object and added 2 resources `users` and nested one: `users/{id}/stories.

The following endpoints are declared with the code above:

```
users/{id}
users/{user_id}/stories/{id}
```

To see all available resources, navigate to `http://0.0.0.0:6543/_`.

Try to navigate to http://0.0.0.0:6543/users or http://0.0.0.0:6543/users/1/stories to see the declared the resources in action.

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
     del Users[id]
```

We need to change the view argument for the `users` resource to point to our new class in the `main`:
```
user = root.add('user', view='myapp.views.UsersView')
```

Restart the server and navigate to http://0.0.0.0:6543/users

Above, we declared `index`, `show`, `create` and `delete` actions which correspond to: GET collection, GET resource, POST resource and DELETE resource respectively. You could also declare `update`, which would correspond to the PUT method. You dont need to declare all of them, only those you need. The missing ones will automatically return 405 Method Not Allowed error.

Comment out the `index` action and try.

Happy RESTing !

