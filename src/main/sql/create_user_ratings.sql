-- Table: user_ratings

-- DROP TABLE user_ratings;

CREATE TABLE user_ratings
(
  id serial NOT NULL,
  user_id character varying NOT NULL,
  package_id character varying NOT NULL,
  rating integer NOT NULL,
  created timestamp without time zone,
  modified timestamp without time zone,
  CONSTRAINT user_ratings_pk PRIMARY KEY (id),
  CONSTRAINT "user_id_package_UNIQUE" UNIQUE (user_id, package_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE user_ratings
  OWNER TO ckan22;

-- Index: user_ratings_idx

-- DROP INDEX user_ratings_idx;

CREATE INDEX user_ratings_idx
  ON user_ratings
  USING btree
  (user_id COLLATE pg_catalog."default", package_id COLLATE pg_catalog."default");

