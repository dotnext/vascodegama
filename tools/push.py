from shell import shell
import uuid
import sys
import webbrowser
from random_words import RandomWords
rw = RandomWords()




route_map = {
    "scaler": "vascodagama-emcworld-scaler",
    "images": "vascodagama-emcworld-prod"
}


domain = "cfapps.io"
base_route = None
base_request = None


if len(sys.argv) < 2:
    print("Not enough arguments - only got {}".format(len(sys.argv)))
    sys.exit(1)

if str(sys.argv[1]).lower() not in route_map.keys():
    print("Failed to specify a known route to modify.  Got '{}', needed: {}".format(sys.argv[1],route_map.keys()))
    sys.exit(1)
else:
    base_route = route_map[str(sys.argv[1]).lower()]
    base_request = str(sys.argv[1]).lower()

print("---------")

random_suffix = "-".join(rw.random_words(count=2))
app_name = base_route+"-"+random_suffix

print("Now creating new app with corresponding name: {}".format(app_name))
command_string = "cf push -f {yaml_base}.yml -n {app_name} {app_name}".format(yaml_base=base_request, app_name=app_name)
print("Using command: {}".format(command_string))
push = shell(command_string)
print(push.output(raw=True))
print("Push Complete - now mapping route")
command_string = "cf map-route {app_name} cfapps.io -n {prod_route}".format(app_name=app_name, prod_route=route_map[base_request])
map_route = shell(command_string)
print(map_route.output(raw=True))
print(command_string)
print("Check it out! Does it look good: http://{}.cfapps.io".format(app_name))
webbrowser.open_new("http://{}.{}".format(app_name,domain))
print("If it looks good, hit enter and we'll roll it back!")

raw_input("Hit Enter!")

print("Deleting App")
command_string = "cf d {app_name} -f".format(app_name=app_name)
delete_app = shell(command_string)
print(delete_app.output(raw=True))
print(command_string)

print("Cleaning up routes")
command_string = "cf delete-route cfapps.io -n {app_name} -f".format(app_name=app_name)
delete_route = shell(command_string)
print(delete_route.output(raw=True))
print(command_string)
