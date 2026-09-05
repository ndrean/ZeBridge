-- ZeBridge scenario baseline schema — the 8 fixture tables, captured from the known-good stack.
-- Regenerate with: pg_dump -s -t public.<each> (see reprovision.py). Reference only; reprovision applies it.

\restrict QMxcuqaIVGorIB87Nw7wBznPafgnMUgSP89cUKYYeNrfJY4T8sYjchtsSCFvkWx
CREATE TABLE public.counter_public (
    uid uuid DEFAULT gen_random_uuid() NOT NULL,
    value integer DEFAULT 0 NOT NULL,
    last_writer character varying(255),
    inserted_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
CREATE TABLE public.counter_tenant (
    uid uuid DEFAULT gen_random_uuid() NOT NULL,
    value integer DEFAULT 0 NOT NULL,
    tenant_id character varying(255) NOT NULL,
    last_writer character varying(255),
    inserted_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
CREATE TABLE public.memo (
    uid uuid DEFAULT gen_random_uuid() NOT NULL,
    txt text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE public.note_t (
    uid uuid NOT NULL,
    txt text,
    tenant_id character varying(255) NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
CREATE TABLE public.orders (
    uid uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id bigint NOT NULL,
    label character varying(255) NOT NULL,
    inserted_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
CREATE TABLE public.salaries (
    uid uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id bigint NOT NULL,
    tenant_id text NOT NULL,
    amount integer NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE public.test_types (
    uid uuid DEFAULT gen_random_uuid() NOT NULL,
    age integer,
    temperature double precision,
    price numeric(20,8),
    is_true boolean,
    some_text text,
    tags text[],
    matrix integer[],
    metadata jsonb,
    deleted_at timestamp with time zone,
    tenant_id character varying(255) NOT NULL,
    last_writer character varying(255),
    inserted_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
CREATE TABLE public.users (
    id bigint NOT NULL,
    name character varying(255) NOT NULL,
    email character varying(255),
    inserted_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
CREATE SEQUENCE public.users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;
ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);
ALTER TABLE ONLY public.counter_public
    ADD CONSTRAINT counter_public_pkey PRIMARY KEY (uid);
ALTER TABLE ONLY public.counter_tenant
    ADD CONSTRAINT counter_tenant_pkey PRIMARY KEY (uid);
ALTER TABLE ONLY public.memo
    ADD CONSTRAINT memo_pkey PRIMARY KEY (uid);
ALTER TABLE ONLY public.note_t
    ADD CONSTRAINT note_t_pkey PRIMARY KEY (uid);
ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (uid);
ALTER TABLE ONLY public.salaries
    ADD CONSTRAINT salaries_pkey PRIMARY KEY (uid);
ALTER TABLE ONLY public.test_types
    ADD CONSTRAINT test_types_pkey PRIMARY KEY (uid);
ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);
CREATE UNIQUE INDEX counter_tenant_zb_ri ON public.counter_tenant USING btree (tenant_id, uid);
ALTER TABLE ONLY public.counter_tenant REPLICA IDENTITY USING INDEX counter_tenant_zb_ri;
CREATE UNIQUE INDEX note_t_zb_ri ON public.note_t USING btree (tenant_id, uid);
ALTER TABLE ONLY public.note_t REPLICA IDENTITY USING INDEX note_t_zb_ri;
CREATE INDEX orders_user_id_idx ON public.orders USING btree (user_id);
CREATE UNIQUE INDEX salaries_zb_ri ON public.salaries USING btree (tenant_id, uid);
ALTER TABLE ONLY public.salaries REPLICA IDENTITY USING INDEX salaries_zb_ri;
CREATE UNIQUE INDEX test_types_zb_ri ON public.test_types USING btree (tenant_id, uid);
ALTER TABLE ONLY public.test_types REPLICA IDENTITY USING INDEX test_types_zb_ri;
CREATE TRIGGER zebridge_bump_version_t BEFORE UPDATE ON public.counter_public FOR EACH ROW EXECUTE FUNCTION public.zebridge_bump_version('updated_at');
CREATE TRIGGER zebridge_bump_version_t BEFORE UPDATE ON public.counter_tenant FOR EACH ROW EXECUTE FUNCTION public.zebridge_bump_version('updated_at');
CREATE TRIGGER zebridge_bump_version_t BEFORE UPDATE ON public.memo FOR EACH ROW EXECUTE FUNCTION public.zebridge_bump_version('updated_at');
CREATE TRIGGER zebridge_bump_version_t BEFORE UPDATE ON public.note_t FOR EACH ROW EXECUTE FUNCTION public.zebridge_bump_version('updated_at');
CREATE TRIGGER zebridge_bump_version_t BEFORE UPDATE ON public.salaries FOR EACH ROW EXECUTE FUNCTION public.zebridge_bump_version('updated_at');
CREATE TRIGGER zebridge_bump_version_t BEFORE UPDATE ON public.test_types FOR EACH ROW EXECUTE FUNCTION public.zebridge_bump_version('updated_at');
CREATE TRIGGER zebridge_guard_tenant_t BEFORE INSERT OR UPDATE ON public.counter_tenant FOR EACH ROW EXECUTE FUNCTION public.zebridge_guard_tenant('tenant_id');
CREATE TRIGGER zebridge_guard_tenant_t BEFORE INSERT OR UPDATE ON public.note_t FOR EACH ROW EXECUTE FUNCTION public.zebridge_guard_tenant('tenant_id');
CREATE TRIGGER zebridge_guard_tenant_t BEFORE INSERT OR UPDATE ON public.salaries FOR EACH ROW EXECUTE FUNCTION public.zebridge_guard_tenant('tenant_id');
CREATE TRIGGER zebridge_guard_tenant_t BEFORE INSERT OR UPDATE ON public.test_types FOR EACH ROW EXECUTE FUNCTION public.zebridge_guard_tenant('tenant_id');
CREATE TRIGGER zebridge_soft_delete_t BEFORE DELETE ON public.test_types FOR EACH ROW EXECUTE FUNCTION public.zebridge_soft_delete('deleted_at', 'updated_at');
CREATE TRIGGER zebridge_width_guard BEFORE INSERT OR UPDATE ON public.memo FOR EACH ROW EXECUTE FUNCTION public.zebridge_width_guard_memo();
CREATE TRIGGER zebridge_width_guard BEFORE INSERT OR UPDATE ON public.note_t FOR EACH ROW EXECUTE FUNCTION public.zebridge_width_guard_note_t();
CREATE TRIGGER zebridge_width_guard BEFORE INSERT OR UPDATE ON public.salaries FOR EACH ROW EXECUTE FUNCTION public.zebridge_width_guard_salaries();
CREATE TRIGGER zebridge_width_guard BEFORE INSERT OR UPDATE ON public.test_types FOR EACH ROW EXECUTE FUNCTION public.zebridge_width_guard_test_types();
ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;
ALTER TABLE ONLY public.salaries
    ADD CONSTRAINT salaries_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;
ALTER TABLE public.counter_tenant ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.note_t ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.salaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.test_types ENABLE ROW LEVEL SECURITY;
CREATE POLICY zb_reader_all ON public.counter_tenant FOR SELECT TO bridge_reader USING (((COALESCE(current_setting('zb.tenant'::text, true), ''::text) = ''::text) OR ((tenant_id)::text = current_setting('zb.tenant'::text, true)) OR ((tenant_id)::text = '_default'::text)));
CREATE POLICY zb_reader_all ON public.note_t FOR SELECT TO bridge_reader USING (((COALESCE(current_setting('zb.tenant'::text, true), ''::text) = ''::text) OR ((tenant_id)::text = current_setting('zb.tenant'::text, true)) OR ((tenant_id)::text = '_default'::text)));
CREATE POLICY zb_reader_all ON public.salaries FOR SELECT TO bridge_reader USING (((COALESCE(current_setting('zb.tenant'::text, true), ''::text) = ''::text) OR (tenant_id = current_setting('zb.tenant'::text, true)) OR (tenant_id = '_default'::text)));
CREATE POLICY zb_reader_all ON public.test_types FOR SELECT TO bridge_reader USING (((COALESCE(current_setting('zb.tenant'::text, true), ''::text) = ''::text) OR ((tenant_id)::text = current_setting('zb.tenant'::text, true)) OR ((tenant_id)::text = '_default'::text)));
CREATE POLICY zb_tenant_write ON public.counter_tenant TO bridge_writer USING ((((tenant_id)::text IN ( SELECT zebridge_user_tenants.tenant_id
   FROM public.zebridge_user_tenants
  WHERE (zebridge_user_tenants.principal = current_setting('zb.principal'::text, true)))) OR ((tenant_id)::text = '_default'::text))) WITH CHECK ((((tenant_id)::text IN ( SELECT zebridge_user_tenants.tenant_id
   FROM public.zebridge_user_tenants
  WHERE (zebridge_user_tenants.principal = current_setting('zb.principal'::text, true)))) OR ((tenant_id)::text = '_default'::text)));
CREATE POLICY zb_tenant_write ON public.note_t TO bridge_writer USING ((((tenant_id)::text IN ( SELECT zebridge_user_tenants.tenant_id
   FROM public.zebridge_user_tenants
  WHERE (zebridge_user_tenants.principal = current_setting('zb.principal'::text, true)))) OR ((tenant_id)::text = '_default'::text))) WITH CHECK ((((tenant_id)::text IN ( SELECT zebridge_user_tenants.tenant_id
   FROM public.zebridge_user_tenants
  WHERE (zebridge_user_tenants.principal = current_setting('zb.principal'::text, true)))) OR ((tenant_id)::text = '_default'::text)));
CREATE POLICY zb_tenant_write ON public.salaries TO bridge_writer USING (((tenant_id IN ( SELECT zebridge_user_tenants.tenant_id
   FROM public.zebridge_user_tenants
  WHERE (zebridge_user_tenants.principal = current_setting('zb.principal'::text, true)))) OR (tenant_id = '_default'::text))) WITH CHECK (((tenant_id IN ( SELECT zebridge_user_tenants.tenant_id
   FROM public.zebridge_user_tenants
  WHERE (zebridge_user_tenants.principal = current_setting('zb.principal'::text, true)))) OR (tenant_id = '_default'::text)));
CREATE POLICY zb_tenant_write ON public.test_types TO bridge_writer USING ((((tenant_id)::text IN ( SELECT zebridge_user_tenants.tenant_id
   FROM public.zebridge_user_tenants
  WHERE (zebridge_user_tenants.principal = current_setting('zb.principal'::text, true)))) OR ((tenant_id)::text = '_default'::text))) WITH CHECK ((((tenant_id)::text IN ( SELECT zebridge_user_tenants.tenant_id
   FROM public.zebridge_user_tenants
  WHERE (zebridge_user_tenants.principal = current_setting('zb.principal'::text, true)))) OR ((tenant_id)::text = '_default'::text)));
\unrestrict QMxcuqaIVGorIB87Nw7wBznPafgnMUgSP89cUKYYeNrfJY4T8sYjchtsSCFvkWx
