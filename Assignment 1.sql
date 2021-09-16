Create table users(userid integer, name text NOT NULL, Primary Key(userid));

Create table movies(movieid integer, title text NOT NULL, Primary Key(movieid));

Create table genres(genreid integer, name text NOT NULL, Primary Key(genreid));

Create table taginfo(tagid integer, content text NOT NULL, Primary Key(tagid));

Create table hasagenre(movieid integer, genreid integer, Primary Key(movieid,genreid), Constraint movieid
					  Foreign Key(movieid) references movies(movieid), Constraint genreid Foreign Key(genreid)
					  references genres(genreid));
					  
					  
Create table tags (userid integer, movieid integer, tagid integer, timestamp bigint NOT NULL, 
				  Primary Key(userid, movieid, tagid), 
				  Constraint userid Foreign Key(userid) references users(userid), 
				  Constraint movieid Foreign Key(movieid) references movies(movieid),
				  Constraint tagid Foreign Key(tagid) references taginfo(tagid));
				  
				  
				  
Create table ratings(userid integer, movieid integer, rating numeric NOT NULL, timestamp bigint NOT NULL, 
					Primary Key(userid, movieid), 
					Constraint userid Foreign Key(userid) references users(userid),
					Constraint movieid Foreign Key(movieid) references movies(movieid),
					CHECK (rating BETWEEN 0 AND 5));