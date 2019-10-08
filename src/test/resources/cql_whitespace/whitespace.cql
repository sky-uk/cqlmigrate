CREATE TABLE role_graphs(provider text, graphml text, PRIMARY KEY (provider));


INSERT into role_graphs (provider, graphml)
    VALUES ('SKY', 'some text; some more text');
-- intentional blank lines below


-- comment