BEGIN;

CREATE TABLE "__STATE__".bottlenecks( LIKE bottlenecks INCLUDING ALL );
ALTER TABLE "__STATE__".bottlenecks ADD CONSTRAINT bottlenecks_state_check CHECK (state='__STATE__'),
      ALTER COLUMN state SET DEFAULT '__STATE__',
      INHERIT bottlenecks;

END;
