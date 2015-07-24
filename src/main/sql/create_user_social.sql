-- Table: user_social

-- DROP TABLE user_social;

CREATE TABLE user_social
(
  id serial NOT NULL,
  ckan_id character varying,
  -- first_name character varying,
  -- last_name character varying,
  -- social_id character varying,
  social character varying,
  details json,
  CONSTRAINT user_social_pk PRIMARY KEY (id),
  CONSTRAINT user_fk FOREIGN KEY (ckan_id)
      REFERENCES public.user (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT user_social_unique UNIQUE (ckan_id, social)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE user_social
  OWNER TO ckan22;

-- Index: fki_users_fk

-- DROP INDEX fki_users_fk;

CREATE INDEX fki_user_fk
  ON user_social
  USING btree
  (ckan_id);

