   -- it's role_graphs_sql table
CREATE TABLE role_graphs_sql(
  provider text, -- provider name
  graphml text, ---graph markup language
  settings text, /** xml markup language **/
  PRIMARY KEY (provider)-- PK
)    --
-- EOF
;