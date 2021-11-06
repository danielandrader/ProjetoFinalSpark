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

beeline -u jdbc:hive2://localhost:10000

create database covid;

use covid;

## Criar tabela externa de staging sem particionamento

create external table tbl_covid_ext(C01 STRING, C02 STRING, C03 STRING, C04 INT, C05 INT, C06 INT, C07 STRING, C08 STRING, C09 INT, C10 INT, C11 INT, C12 INT, C13 INT, C14 INT, C15 INT, C16 INT, C17 INT) partitioned by (municipio string) row format delimited fields terminated by ';' lines terminated by '\n' stored as textfile location '/user/daniel/data/covid';

describe tbl_covid_ext;

## Ajustes para preparar o ambiente para criação de partição dinâmica

SET hive.exec.dynamic.partition=true; 
SET hive.exec.dynamic.partition.mode=nonstrict;

SET hive.exec.max.dynamic.partitions.pernode = 1000000;
SET hive.exec.max.dynamic.partitions = 1000000;
SET hive.exec.max.created.files = 1000000;

## Criar tabela com particionamento

create external table tbl_covid_part(C01 STRING, C02 STRING, C03 STRING, C04 INT, C05 INT, C06 INT, C07 STRING, C08 STRING, C09 INT, C10 INT, C11 INT, C12 INT, C13 INT, C14 INT, C15 INT, C16 INT, C17 INT) partitioned by (MUNICIPIO string) row format delimited fields terminated by ';' lines terminated by '\n' stored as textfile;

## Inserir dados já com o particionamento dinâmico

insert overwrite table tbl_covid_part partition (MUNICIPIO) select C01, C02, C03, C04, C05, C06, C07, C08, C09, C10, C11, C12, C13, C14, C15, C16, C17, C03 from tbl_covid_ext distribute by C03;

## Exemplo de inserir dados com particionamento manual

insert overwrite table tbl_covid_part partition (MUNICIPIO='Santos') select C01, C02, C03, C04, C05, C06, C07, C08, C09, C10, C11, C12, C13, C14, C15, C16, C17 from tbl_covid_ext where C03 = 'Santos';

---


1. Enviar os dados para o hdfs



2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município.
