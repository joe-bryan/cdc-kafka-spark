-- Table: public.rental_streaming

-- DROP TABLE IF EXISTS public.rental_streaming;

CREATE TABLE IF NOT EXISTS public.rental_streaming
(
    rental_id integer,
    rental_date timestamp,
    inventory_id integer,
    customer_id smallint,
    return_date timestamp,
    staff_id smallint,
    last_update timestamp
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.rental_streaming
    OWNER to root;