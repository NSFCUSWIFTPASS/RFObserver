-- rf-db's schema, copied from rf-processor db/schema.sql (784bb08, 2025-10-26).
-- The rf-db writer's integration tests load it into a scratch database.

--
-- PostgreSQL database dump
--

-- Dumped from database version 16.2 (Debian 16.2-1.pgdg120+2)
-- Dumped by pg_dump version 16.2 (Debian 16.2-1.pgdg120+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_stat_statements; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public;


--
-- Name: EXTENSION pg_stat_statements; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_stat_statements IS 'track execution statistics of all SQL statements executed';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: hardware; Type: TABLE; Schema: public; Owner: nrdz
--

CREATE TABLE public.hardware (
    hardware_id integer NOT NULL,
    location character varying(100) NOT NULL,
    enclosure boolean NOT NULL,
    op_status integer NOT NULL,
    mount_id integer NOT NULL
);


ALTER TABLE public.hardware OWNER TO nrdz;

--
-- Name: hardware_hardware_id_seq; Type: SEQUENCE; Schema: public; Owner: nrdz
--

ALTER TABLE public.hardware ALTER COLUMN hardware_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.hardware_hardware_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: metadata; Type: TABLE; Schema: public; Owner: nrdz
--

CREATE TABLE public.metadata (
    metadata_id integer NOT NULL,
    frequency bigint NOT NULL,
    sample_rate bigint NOT NULL,
    bandwidth bigint NOT NULL,
    gain integer NOT NULL,
    length numeric NOT NULL,
    "interval" numeric NOT NULL,
    bit_depth character varying(10)
);


ALTER TABLE public.metadata OWNER TO nrdz;

--
-- Name: metadata_metadata_id_seq; Type: SEQUENCE; Schema: public; Owner: nrdz
--

ALTER TABLE public.metadata ALTER COLUMN metadata_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.metadata_metadata_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: outputs; Type: TABLE; Schema: public; Owner: nrdz
--

CREATE TABLE public.outputs (
    output_id bigint NOT NULL,
    hardware_id integer NOT NULL,
    metadata_id integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    average_db numeric(21,16) NOT NULL,
    max_db numeric(21,16) NOT NULL,
    median_db numeric(21,16) NOT NULL,
    std_dev numeric NOT NULL,
    kurtosis numeric NOT NULL
);


ALTER TABLE public.outputs OWNER TO nrdz;

--
-- Name: outputs_output_id_seq; Type: SEQUENCE; Schema: public; Owner: nrdz
--

ALTER TABLE public.outputs ALTER COLUMN output_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.outputs_output_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: rpi; Type: TABLE; Schema: public; Owner: nrdz
--

CREATE TABLE public.rpi (
    rpi_id integer NOT NULL,
    hostname character varying(100) NOT NULL,
    rpi_ip inet NOT NULL,
    rpi_mac macaddr NOT NULL,
    rpi_v character varying(255) NOT NULL,
    os_v character varying(255) NOT NULL,
    memory bigint NOT NULL,
    storage_cap bigint NOT NULL,
    cpu_type character varying(255) NOT NULL,
    cpu_cores integer NOT NULL,
    op_status integer NOT NULL,
    hardware_id integer NOT NULL
);


ALTER TABLE public.rpi OWNER TO nrdz;

--
-- Name: rpi_rpi_id_seq; Type: SEQUENCE; Schema: public; Owner: nrdz
--

ALTER TABLE public.rpi ALTER COLUMN rpi_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.rpi_rpi_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: sdr; Type: TABLE; Schema: public; Owner: nrdz
--

CREATE TABLE public.sdr (
    sdr_id integer NOT NULL,
    sdr_serial character(7) NOT NULL,
    mboard_name character varying(255) NOT NULL,
    external_clock boolean NOT NULL,
    op_status integer NOT NULL,
    hardware_id integer NOT NULL
);


ALTER TABLE public.sdr OWNER TO nrdz;

--
-- Name: sdr_sdr_id_seq; Type: SEQUENCE; Schema: public; Owner: nrdz
--

ALTER TABLE public.sdr ALTER COLUMN sdr_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.sdr_sdr_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: sensor_names; Type: TABLE; Schema: public; Owner: nrdz
--

CREATE TABLE public.sensor_names (
    name_id integer NOT NULL,
    sensor_name character varying(100) NOT NULL,
    description text,
    hardware_id integer NOT NULL
);


ALTER TABLE public.sensor_names OWNER TO nrdz;

--
-- Name: sensor_names_name_id_seq; Type: SEQUENCE; Schema: public; Owner: nrdz
--

ALTER TABLE public.sensor_names ALTER COLUMN name_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.sensor_names_name_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: storage; Type: TABLE; Schema: public; Owner: nrdz
--

CREATE TABLE public.storage (
    mount_id integer NOT NULL,
    nfs_mnt character varying(255) NOT NULL,
    local_mnt character varying(255) NOT NULL,
    storage_cap bigint NOT NULL,
    op_status integer NOT NULL
);


ALTER TABLE public.storage OWNER TO nrdz;

--
-- Name: storage_mount_id_seq; Type: SEQUENCE; Schema: public; Owner: nrdz
--

ALTER TABLE public.storage ALTER COLUMN mount_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.storage_mount_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: wrlen; Type: TABLE; Schema: public; Owner: nrdz
--

CREATE TABLE public.wrlen (
    wr_id integer NOT NULL,
    wr_serial character varying(100),
    wr_ip inet,
    wr_mac macaddr,
    mode character varying(100),
    wr_host character varying(100),
    op_status integer NOT NULL,
    hardware_id integer NOT NULL
);


ALTER TABLE public.wrlen OWNER TO nrdz;

--
-- Name: wrlen_wr_id_seq; Type: SEQUENCE; Schema: public; Owner: nrdz
--

ALTER TABLE public.wrlen ALTER COLUMN wr_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.wrlen_wr_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: hardware hardware_pkey; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.hardware
    ADD CONSTRAINT hardware_pkey PRIMARY KEY (hardware_id);


--
-- Name: metadata metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.metadata
    ADD CONSTRAINT metadata_pkey PRIMARY KEY (metadata_id);


--
-- Name: metadata metadata_unique_key; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.metadata
    ADD CONSTRAINT metadata_unique_key UNIQUE (frequency, sample_rate, bandwidth, gain, length, "interval", bit_depth);


--
-- Name: outputs outputs_event_unique; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.outputs
    ADD CONSTRAINT outputs_event_unique UNIQUE (hardware_id, metadata_id, created_at);


--
-- Name: outputs outputs_pkey; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.outputs
    ADD CONSTRAINT outputs_pkey PRIMARY KEY (output_id);


--
-- Name: rpi rpi_hostname_key; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.rpi
    ADD CONSTRAINT rpi_hostname_key UNIQUE (hostname);


--
-- Name: rpi rpi_pkey; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.rpi
    ADD CONSTRAINT rpi_pkey PRIMARY KEY (rpi_id);


--
-- Name: sdr sdr_pkey; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.sdr
    ADD CONSTRAINT sdr_pkey PRIMARY KEY (sdr_id);


--
-- Name: sdr sdr_sdr_serial_key; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.sdr
    ADD CONSTRAINT sdr_sdr_serial_key UNIQUE (sdr_serial);


--
-- Name: sensor_names sensor_names_pkey; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.sensor_names
    ADD CONSTRAINT sensor_names_pkey PRIMARY KEY (name_id);


--
-- Name: sensor_names sensor_names_sensor_name_key; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.sensor_names
    ADD CONSTRAINT sensor_names_sensor_name_key UNIQUE (sensor_name);


--
-- Name: storage storage_nfs_mnt_key; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.storage
    ADD CONSTRAINT storage_nfs_mnt_key UNIQUE (nfs_mnt);


--
-- Name: storage storage_pkey; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.storage
    ADD CONSTRAINT storage_pkey PRIMARY KEY (mount_id);


--
-- Name: wrlen wrlen_pkey; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.wrlen
    ADD CONSTRAINT wrlen_pkey PRIMARY KEY (wr_id);


--
-- Name: wrlen wrlen_wr_serial_key; Type: CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.wrlen
    ADD CONSTRAINT wrlen_wr_serial_key UNIQUE (wr_serial);


--
-- Name: hardware_metadata_idx; Type: INDEX; Schema: public; Owner: nrdz
--

CREATE INDEX hardware_metadata_idx ON public.outputs USING btree (hardware_id, metadata_id);


--
-- Name: outputs_created_at_idx; Type: INDEX; Schema: public; Owner: nrdz
--

CREATE INDEX outputs_created_at_idx ON public.outputs USING btree (created_at);


--
-- Name: sensor_names fk_hardware; Type: FK CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.sensor_names
    ADD CONSTRAINT fk_hardware FOREIGN KEY (hardware_id) REFERENCES public.hardware(hardware_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: hardware hardware_mount_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.hardware
    ADD CONSTRAINT hardware_mount_id_fkey FOREIGN KEY (mount_id) REFERENCES public.storage(mount_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: outputs outputs_hardware_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.outputs
    ADD CONSTRAINT outputs_hardware_id_fkey FOREIGN KEY (hardware_id) REFERENCES public.hardware(hardware_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: outputs outputs_metadata_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.outputs
    ADD CONSTRAINT outputs_metadata_id_fkey FOREIGN KEY (metadata_id) REFERENCES public.metadata(metadata_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: rpi rpi_hardware_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.rpi
    ADD CONSTRAINT rpi_hardware_id_fkey FOREIGN KEY (hardware_id) REFERENCES public.hardware(hardware_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: sdr sdr_hardware_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.sdr
    ADD CONSTRAINT sdr_hardware_id_fkey FOREIGN KEY (hardware_id) REFERENCES public.hardware(hardware_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: wrlen wrlen_hardware_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: nrdz
--

ALTER TABLE ONLY public.wrlen
    ADD CONSTRAINT wrlen_hardware_id_fkey FOREIGN KEY (hardware_id) REFERENCES public.hardware(hardware_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

