import json  # This is a handy library for doing JSON work.
import \
    os  # We import os here in order to manage environment variables for the tutorial. You don't need to do this on a local system or anywhere you can more conveniently set environment variables.

import looker_sdk  # Note that the pip install required a hyphen but the import is an underscore.

os.environ[
    "LOOKERSDK_BASE_URL"] = "https://nuts.looker.com"  # If your looker URL has .cloud in it (hosted on GCP), do not include :19999 (ie: https://your.cloud.looker.com).
os.environ["LOOKERSDK_API_VERSION"] = "3.1"  # 3.1 is the default version. You can change this to 4.0 if you want.
os.environ[
    "LOOKERSDK_VERIFY_SSL"] = "true"  # Defaults to true if not set. SSL verification should generally be on unless you have a real good reason not to use it. Valid options: true, y, t, yes, 1.
os.environ["LOOKERSDK_TIMEOUT"] = "3600"  # Seconds till request timeout. Standard default is 120.

# Get the following values from your Users page in the Admin panel of your Looker instance > Users > Your user > Edit API keys. If you know your user id, you can visit https://your.looker.com/admin/users/<your_user_id>/edit.
os.environ["LOOKERSDK_CLIENT_ID"] = "GwBwWqXJdDS6X2QprcFh"  # No defaults.
os.environ[
    "LOOKERSDK_CLIENT_SECRET"] = "ncSvf6NgNZMzhYh7YxxZw7kj"  # No defaults. This should be protected at all costs. Please do not leave it sitting here, even if you don't share this document.

print("All environment variables set.")

sdk = looker_sdk.init31()
print('Looker SDK 3.1 initialized successfully.')

# Uncomment out the lines below if you want to instead initialize the 4.0 SDK. It's that easy— Just replace init31 with init40.
# sdk = looker_sdk.init40()
# print('Looker SDK 4.0 initialized successfully.')

my_user = sdk.me()

# Output is an instance of the User model, but can also be read like a python dict. This applies to all Looker API calls that return Models.
# Example: The following commands return identical output. Feel free to use whichever style is more comfortable for you.

print(my_user.first_name)  # Model dot notation
print(my_user["first_name"])  # Dictionary

# Enter your Look ID. If your URL is https://your.cloud.looker.com/looks/25, your Look ID is 25.
look_id = 2248
look = sdk.look(look_id=look_id)
# This gives us a Look object. We'll print the ID of it to verify everything's working.

print(look.id)

# You actually don't need to do anything further for this case, using a Look.
# If you wanted to use an Explore instead, you'd have to get the underlying query first, which might look like this:

# explore_id = "Q4pXny1FEtuxMuj9Atf0Gg"
# If your URL looks like https://your.cloud.looker.com/explore/ecommerce_data/order_items?qid=Q4pXny1FEtuxMuj9Atf0Gg&origin_space=15&toggle=vis, your explore_id/QID is Q4pXny1FEtuxMuj9Atf0Gg.
# explore_query = sdk.query_for_slug(slug=explore_id)

# This would return a Query object that we could then run to get results in step 2 using the run_query endpoints.

# We'll use a try/except block here, to make debugging easier.
# In general, this kind of thing isn't really necessary in notebooks as each cell is already isolated from the rest,
# but it's a good practice in larger scripts and certainly in applications where fatal errors can break the entire app.
# You should get into the habit of using them.

try:
    response = sdk.run_look(
        look_id=look.id,
        result_format="csv"
        # Options here are csv, json, json_detail, txt, html, md, xlsx, sql (returns the raw query), png, jpg. JSON is the easiest to work with in python, so we return it.
    )
    data = json.loads(
        response)  # The response is just a string, so we have to use the json library to load it as a json dict.
    print(data)  # If our query was successful we should see an array of rows.
except:
    raise Exception(f'Error running look {look.id}')

# Before we move on, here's a simple example of that. Let's print the first 10 rows.
# This script is set up to always only look at the first column, assuming our Look returns 1 column.
first_field = list(
    data[0].keys()
)[
    0]  # This looks at the first row of the data and returns the first field name. keys() returns a set, so we wrap it in list() to return an array.

for i in range(0, 10):
    print(i, data[i][first_field])

# If we _know_ the name of the first field, why did we go to all this list(data[0].keys()[0]) trouble? Well, we know the name of the first field for ONE look.
# This little trickery above makes it so that our script will always work for any Look, no matter what the name is, without having to edit the code.
