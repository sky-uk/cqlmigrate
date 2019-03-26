CREATE TABLE cycling.cyclist_alt_stats ( id UUID PRIMARY KEY, lastname text, birthday timestamp, nationality text, weight text, height text );

CREATE TABLE cycling.whimsey ( id UUID PRIMARY KEY, lastname text, cyclist_teams set<text>, events list<text>, teams map<int,text> );

CREATE TABLE cycling.route (race_id int, race_name text, point_id int, lat_long tuple<text, tuple<float,float>>, PRIMARY KEY (race_id, point_id));
