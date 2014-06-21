"""A class to convert longitudes and latitudes in to country/city/state.
This is the same conversion method that Skout currently uses."""

__author__ = "Nicolas, Matias, GOnzalo"
__version__ = "0.1"

import cPickle
import math
import os

# Relative location of the geolocation conversion file to this file
GEOFILE = 'data/geo.cp'


class GeoConversion(object):
    """This object loads a geo database that we use to convert a lat/lon
    location in to a location string."""
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), GEOFILE)) as geo_data:
            self.locations = cPickle.load(geo_data)

    def convert(self, lat, lon):
        """Convert a latitude and longitude in to a location string.
        The output is [country, city, state]
        If no viable results are found, the function will return None."""

        lat, lon = float(lat), float(lon)
        positions = []

        def distance(pos):
            return (math.pow(lat - pos[0], 2)
                    + math.pow(lon - pos[1], 2)
                    + 1 / math.log10(pos[2]))

        for dlat in xrange(-1, 2):
            for dlon in xrange(-1, 2):
                key = "{}_{}".format(int(lat) + dlat, int(lon) + dlon)
                positions += self.locations[key]

        if len(positions) == 0:
            return None
        else:
            return sorted(positions, key=distance)[0][3:6]
