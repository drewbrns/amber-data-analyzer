"""
 Nearest Road Service - This service associates coordinates (lon,lat) with the road it is closest to.
"""


import psycopg2


class NearestRoad(object):
    """ """

    def __init__(self, data):

        self.db = psycopg2.connect(dbname="gis", user=data.username)

    def match(self, datapoint):
        """ """
        query = """ SELECT
                        osm_id, 
                        ST_DistanceSphere( ST_Transform(r.way, 4326), ST_SetSRID(ST_MakePoint(%(lon)s,%(lat)s), 4326) )
                    FROM 
                        planet_osm_line r
                    WHERE 
                        (
                            highway = 'trunk' OR highway = 'primary' OR 
                            highway = 'secondary' OR highway = 'tertiary' OR  
                            highway = 'unclassified' OR highway = 'residential' OR 
                            highway = 'service' OR highway = 'living_street' OR 
                            highway = 'motorway' OR highway = 'motorway_link' OR
                            highway = 'trunk_link' OR highway = 'primary_link' OR 
                            highway = 'secondary_link' OR highway = 'tertiary_link' OR 
                            highway = 'unclassified_link' OR highway = 'residential_link' OR                             
                            highway = 'living_street_link'
                        ) 
                    ORDER BY 2
                    LIMIT 1;
        """

        cursor = self.db.cursor()
        cursor.execute(
            query, {'lon': datapoint['longitude'], 'lat': datapoint['latitude']}
        )
        result = cursor.fetchone()
        datapoint['road'] = "{}".format(result[0])
        return datapoint
