DDL_QUERY =  '''
-- Crear tabla dim_companies
CREATE TABLE IF NOT EXISTS dim_companies (
  symbol varchar(6) NOT NULL,
  exchange varchar(6),
  shortname varchar(100),
  longname varchar(100),
  sector varchar(100),
  industry varchar(50),
  currentprice float,
  marketcap int8,
  ebitda int8,
  revenuegrowth float,
  city varchar(100),
  state varchar(10),
  country varchar(30),
  fulltimeemployees int4,
  weight float,
  PRIMARY KEY (Symbol)
);

-- Crear tabla dim_date
CREATE TABLE IF NOT EXISTS dim_date (
  id_fecha varchar(20) NOT NULL,
  year int,
  month int,
  quarter int,
  day int,
  week int,
  dayofweek int,
  PRIMARY KEY (id_fecha)
);

-- Crear tabla SP500
CREATE TABLE IF NOT EXISTS SP500 (
  id_fecha varchar(20),
  SP500 float,
  PRIMARY KEY (id_fecha),
  FOREIGN KEY (id_fecha) REFERENCES dim_date(id_fecha)
);

-- Crear tabla principal
CREATE TABLE IF NOT EXISTS stock_daily (
  id varchar(20),
  Symbol varchar(6),
  Adj_Close float,
  Close float,
  High float,
  Low float,
  Open float,
  Volume int,
  id_fecha varchar(20),
  PRIMARY KEY (id),
  FOREIGN KEY (Symbol) REFERENCES dim_companies(Symbol),
  FOREIGN KEY (id_fecha) REFERENCES dim_date(id_fecha)
);

 '''