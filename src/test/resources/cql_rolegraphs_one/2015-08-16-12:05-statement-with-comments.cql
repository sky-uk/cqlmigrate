INSERT into role_graphs (provider, graphml)
  VALUES ('SKY', 'some text; some more text');

   -- it's role_graphs_sql table
CREATE TABLE role_graphs_sql(
  provider text, -- provider name
  graphml text, ---graph markup language
  settings text, /** xml markup language **/
  PRIMARY KEY (provider)-- PK
) WITH comment='test table role_graphs_sql'  --
-- EOF
;

INSERT into role_graphs_sql (provider, graphml)
    VALUES ('SKY', 'some ...  
-- it''s comment
-- Created by yEd 3.12.2 /* <key for="graphml" id="d0" yfiles.type="resources"/> */ <test>''</test>
   the end ')
;

/* /* Multi-line comment */
INSERT into role_graphs_sql (provider, graphml, settings)
    VALUES ('EARTH', '', '<empty />');
  */ 

INSERT into role_graphs_sql (provider, graphml, settings)
    VALUES ('EARTH', '',
'   the end ');