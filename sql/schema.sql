    CREATE TABLE earthquake_events (
        -- идентификация
        event_id        text PRIMARY KEY,           -- id из GeoJSON, например "us7000abcd"
        net             text,                       -- сеть, которая предоставила основную версию
        code            text,                       -- код события внутри этой сети
        ids             text[],                     -- все id, если событие склеено из нескольких
        sources         text[],                     -- все сети-источники

        -- что и когда
        event_type      text NOT NULL,              -- earthquake, quarry blast, explosion...
        event_time      timestamptz NOT NULL,       -- из properties.time (мс epoch)
        updated_at      timestamptz NOT NULL,       -- из properties.updated (мс epoch)
        status          text,                       -- automatic / reviewed / deleted

        -- где
        longitude       double precision NOT NULL,
        latitude        double precision NOT NULL,
        depth_km        double precision,           -- бывает отрицательной (выше уровня моря)
        place           text,                       -- свободный текст, для отображения

        -- сила и последствия
        mag             double precision,           -- бывает NULL
        mag_type        text,                       -- ml, md, mb, mww...
        sig             integer,                    -- значимость события (0–1000+)
        felt            integer,                    -- число отчётов «я почувствовал»
        cdi             double precision,           -- интенсивность по отчётам людей
        mmi             double precision,           -- расчётная интенсивность
        alert           text,                       -- green/yellow/orange/red, обычно NULL
        tsunami         boolean,                    -- в фиде это 0/1

        -- качество измерения
        nst             integer,                    -- число станций
        gap             double precision,           -- азимутальный разрыв, градусы
        dmin            double precision,           -- расстояние до ближайшей станции
        rms             double precision,           -- среднеквадратичная ошибка

        url             text,                       -- страница события на сайте USGS

        -- технические поля пайплайна
        first_seen_at   timestamptz NOT NULL DEFAULT now(),  -- когда событие впервые попало к нам
        last_loaded_at  timestamptz NOT NULL DEFAULT now()   -- когда строку последний раз перезаписали
    );

    CREATE INDEX idx_events_event_time ON earthquake_events (event_time);
    CREATE INDEX idx_events_updated_at ON earthquake_events (updated_at);