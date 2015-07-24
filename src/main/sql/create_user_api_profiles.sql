
CREATE TABLE public.user_api_profiles
(
  user_id character varying NOT NULL,
  api character varying NOT NULL,
  profile character varying NOT NULL,
  created timestamp without time zone,
  modified timestamp without time zone,
  CONSTRAINT user_api_profiles_pk PRIMARY KEY (user_id, api)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.user_api_profiles
  OWNER TO ckan22;

CREATE INDEX user_api_profile_idx
  ON public.user_api_profiles
  USING btree
  (user_id COLLATE pg_catalog."default");

CREATE INDEX user_api_idx
  ON public.user_api_profiles
  USING btree
  (api COLLATE pg_catalog."default");

CREATE INDEX user_profile_idx
  ON public.user_api_profiles
  USING btree
  (profile COLLATE pg_catalog."default");


