#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
from geopy import distance
from math import sin, cos, sqrt, atan2, radians

def manual_distance(lat2, lon2, lat1, lon1):
    R = 3959 # miles
    var1 = radians(lat1)
    var2 = radians(lat2)
    delta_var = radians(lat2-lat1)
    lambda_val = radians(lon2-lon1)
    a = sin(delta_var/2) * sin(delta_var/2) + cos(var1) * cos(var2) * sin(lambda_val/2) * sin(lambda_val/2)
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    if (cityToSearch == None or saveLocation1 == None or collection == None):
        return

    businesses = collection.find({"city": { "$regex" : "^" + cityToSearch + "$" , "$options" : "i"} },
                                 {"name":1, "full_address":1,"city":1, "state":1})
    if businesses is None:
        return

    write_lines = []
    for business in businesses:
        name = business['name'].upper()
        full_address = business['full_address'].upper()
        city = business['city'].upper()
        state = business['state'].upper()

        write_line = name + "$" + full_address + "$" + city + "$" + state + "\n"
        write_lines.append(write_line)

    with open(saveLocation1, 'w') as f:
        f.writelines(write_lines)

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    if (categoriesToSearch == None or myLocation == None or maxDistance == None or saveLocation2 == None or collection == None):
        return

    locationsWithTags = collection.find({'categories' : { "$in" : categoriesToSearch }})
    if (locationsWithTags == None):
        return

    locationNames = []
    current_location = (float(myLocation[0]), float(myLocation[1]))
    for location in locationsWithTags:
        location_name = location['name'].upper()
        location_lat = location['latitude']
        location_long = location['longitude']
        location_coord = (location_lat, location_long)

        # d = distance.distance(current_location, location_coord).miles
        d = manual_distance(current_location[0], current_location[1], location_lat, location_long)
        if (d > maxDistance):
            continue

        locationNames.append(location_name + "\n")

    if len(locationNames) == 0:
        return

    with open(saveLocation2, 'w') as f:
        f.writelines(locationNames)