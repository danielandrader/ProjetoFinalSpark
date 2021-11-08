# ProjetoFinalSpark
Projeto desenvolvido no treinamento de Engenharia de Dados ministrado pela Semantix Academy

## Instalar o pacote do repositório epel 
yum install epel-release

## Instalar os descompactadores
yum install unar
yum install p7zip

## Descompactar o arquivo RAR disponibilizado 
unar HIST_PAINEL_COVIDBR_04nov2021.rar

## Descompactar o arquivo 7z disponibilizado no site covid.gov.br (5 de novembro)
7za x HIST_PAINEL_COVIDBR_05nov2021.7z

## Transferir os dados da VM para o container namenode
docker cp HIST_PAINEL_COVIDBR_04nov2021 namenode:/input
docker cp covid namenode:/input

## Entrar no terminal do container namenode
docker exec -it namenode bash

## Criar o diretório no HDFS
hdfs dfs -mkdir -p /user/daniel/data/covid

## Transferir do filesystem local (namenode) para o HDFS
hdfs dfs -put /input/HIST_PAINEL_COVIDBR_04nov2021/ /user/daniel/data
hdfs dfs -put /input/covid /user/daniel/data

## Listar os arquivos do HDFShdfs 
dfs -ls /user/daniel/data

root@namenode:/input/HIST_PAINEL_COVIDBR_04nov2021# hdfs dfs -ls /user/daniel/data
Found 1 items
drwxr-xr-x   - root supergroup          0 2021-11-05 13:44 /user/daniel/data/HIST_PAINEL_COVIDBR_04nov2021

root@namenode:/input/HIST_PAINEL_COVIDBR_04nov2021# hdfs dfs -ls /user/daniel/data/HIST_PAINEL_COVIDBR_04nov2021
Found 4 items
-rw-r--r--   3 root supergroup   62492959 2021-11-05 13:44 /user/daniel/data/HIST_PAINEL_COVIDBR_04nov2021/HIST_PAINEL_COVIDBR_2020_Parte1_04nov2021.csv
-rw-r--r--   3 root supergroup   76520606 2021-11-05 13:44 /user/daniel/data/HIST_PAINEL_COVIDBR_04nov2021/HIST_PAINEL_COVIDBR_2020_Parte2_04nov2021.csv
-rw-r--r--   3 root supergroup   91120853 2021-11-05 13:44 /user/daniel/data/HIST_PAINEL_COVIDBR_04nov2021/HIST_PAINEL_COVIDBR_2021_Parte1_04nov2021.csv
-rw-r--r--   3 root supergroup   64477583 2021-11-05 13:44 /user/daniel/data/HIST_PAINEL_COVIDBR_04nov2021/HIST_PAINEL_COVIDBR_2021_Parte2_04nov2021.csv

root@namenode:/# hdfs dfs -ls /user/daniel/data/covid
Found 4 items
-rw-r--r--   3 root supergroup   62492959 2021-11-04 23:48 /user/daniel/data/covid/HIST_PAINEL_COVIDBR_2020_Parte1_05nov2021.csv
-rw-r--r--   3 root supergroup   76520606 2021-11-04 23:48 /user/daniel/data/covid/HIST_PAINEL_COVIDBR_2020_Parte2_05nov2021.csv
-rw-r--r--   3 root supergroup   91120853 2021-11-04 23:48 /user/daniel/data/covid/HIST_PAINEL_COVIDBR_2021_Parte1_05nov2021.csv
-rw-r--r--   3 root supergroup   64985503 2021-11-04 23:48 /user/daniel/data/covid/HIST_PAINEL_COVIDBR_2021_Parte2_05nov2021.csv

## Container hive-server
exec -it hive-server bash

beeline> root@hive_server:/opt# beeline -u jdbc:hive2://localhost:10000
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-2.7.4/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Connecting to jdbc:hive2://localhost:10000
Connected to: Apache Hive (version 2.3.2)
Driver: Hive JDBC (version 2.3.2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 2.3.2 by Apache Hive


create database covid;

0: jdbc:hive2://localhost:10000> use covid;
No rows affected (0.074 seconds)


## Criar tabela externa de staging sem particionamento

0: jdbc:hive2://localhost:10000> create external table tbl_covid_ext(regiao STRING, estado STRING, municipio STRING, coduf INT, codmun INT, codRegiaoSaude INT, nomeRegiaoSaude STRING, data DATE, semanaEpi INT, populacaoTCU2019 INT, casosAcumulado INT, casosNovos INT, obitosAcumulado INT, obitosNovos INT, Recuperadosnovos INT, emAcompanhamentoNovos INT, interior_metropolitana INT) row format delimited fields terminated by ';' lines terminated by '\n' stored as textfile location '/user/daniel/data/HIST_PAINEL_COVIDBR_04nov2021';
No rows affected (0.246 seconds)

0: jdbc:hive2://localhost:10000> select count(*) from tbl_covid_ext;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+----------+
|   _c0    |
+----------+
| 3304844  |
+----------+
1 row selected (16.965 seconds)


0: jdbc:hive2://localhost:10000> describe tbl_covid_ext;
+-------------------------+------------+----------+
|        col_name         | data_type  | comment  |
+-------------------------+------------+----------+
| regiao                  | string     |          |
| estado                  | string     |          |
| municipio               | string     |          |
| coduf                   | int        |          |
| codmun                  | int        |          |
| codregiaosaude          | int        |          |
| nomeregiaosaude         | string     |          |
| data                    | date       |          |
| semanaepi               | int        |          |
| populacaotcu2019        | int        |          |
| casosacumulado          | int        |          |
| casosnovos              | int        |          |
| obitosacumulado         | int        |          |
| obitosnovos             | int        |          |
| recuperadosnovos        | int        |          |
| emacompanhamentonovos   | int        |          |
| interior_metropolitana  | int        |          |
+-------------------------+------------+----------+
17 rows selected (0.111 seconds)



## Ajustes para preparar o ambiente para criação de partição dinâmica

SET hive.exec.dynamic.partition=true; 
SET hive.exec.dynamic.partition.mode=nonstrict;

SET hive.exec.max.dynamic.partitions.pernode = 1000000;
SET hive.exec.max.dynamic.partitions = 1000000;
SET hive.exec.max.created.files = 1000000;

## Criar tabela com particionamento

0: jdbc:hive2://localhost:10000> create table tbl_covid_part(regiao STRING, estado STRING, coduf INT, codmun INT, codRegiaoSaude INT, nomeRegiaoSaude STRING, data DATE, semanaEpi INT, populacaoTCU2019 INT, casosAcumulado INT, casosNovos INT, obitosAcumulado INT, obitosNovos INT, Recuperadosnovos INT, emAcompanhamentoNovos INT, interior_metropolitana INT) partitioned by (MUNICIPIO string) stored as textfile;
No rows affected (0.343 seconds)

0: jdbc:hive2://localhost:10000> select count(*) from tbl_covid_part;
+------+
| _c0  |
+------+
| 0    |
+------+
1 row selected (0.672 seconds)

0: jdbc:hive2://localhost:10000> describe tbl_covid_part;
+--------------------------+-----------------------+-----------------------+
|         col_name         |       data_type       |        comment        |
+--------------------------+-----------------------+-----------------------+
| regiao                   | string                |                       |
| estado                   | string                |                       |
| coduf                    | int                   |                       |
| codmun                   | int                   |                       |
| codregiaosaude           | int                   |                       |
| nomeregiaosaude          | string                |                       |
| data                     | date                  |                       |
| semanaepi                | int                   |                       |
| populacaotcu2019         | int                   |                       |
| casosacumulado           | int                   |                       |
| casosnovos               | int                   |                       |
| obitosacumulado          | int                   |                       |
| obitosnovos              | int                   |                       |
| recuperadosnovos         | int                   |                       |
| emacompanhamentonovos    | int                   |                       |
| interior_metropolitana   | int                   |                       |
| municipio                | string                |                       |
|                          | NULL                  | NULL                  |
| # Partition Information  | NULL                  | NULL                  |
| # col_name               | data_type             | comment               |
|                          | NULL                  | NULL                  |
| municipio                | string                |                       |
+--------------------------+-----------------------+-----------------------+
22 rows selected (0.569 seconds)


## Inserir dados já com o particionamento dinâmico

0: jdbc:hive2://localhost:10000> insert overwrite table tbl_covid_part partition (MUNICIPIO) select regiao, estado, coduf, codmun, codRegiaoSaude, nomeRegiaoSaude, data, semanaEpi, populacaoTCU2019, casosAcumulado, casosNovos, obitosAcumulado, obitosNovos, Recuperadosnovos, emAcompanhamentoNovos, interior_metropolitana, municipio from tbl_covid_ext;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.

0: jdbc:hive2://localhost:10000> select count(*) from tbl_covid_part;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+----------+
|   _c0    |
+----------+
| 3304844  |
+----------+





