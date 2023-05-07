# Trabalho final de Mineração de Dados 2023

UnB - PPCA

Professor Ladeira

Link - https://github.com/gui1080/DataMining-2023

A pasta de datasets foi adicionada ao "gitignore" para não ser upada, junto da **key** (arquivo em formato *json*) que se conecta ao Google Big Query.

# Estrutura da pasta de dados

````
.
├── 0-CNPJ (sqlite)
│   ├── 202211
│   │   ├── .DS_Store
│   │   ├── Cnaes.zip
│   │   ├── Empresas0.zip
│   │   ├── Empresas1.zip
│   │   ├── Empresas2.zip
│   │   ├── Empresas3.zip
│   │   ├── Empresas4.zip
│   │   ├── Empresas5.zip
│   │   ├── Empresas6.zip
│   │   ├── Empresas7.zip
│   │   ├── Empresas8.zip
│   │   ├── Empresas9.zip
│   │   ├── Estabelecimentos0.zip
│   │   ├── Estabelecimentos1.zip
│   │   ├── Estabelecimentos2.zip
│   │   ├── Estabelecimentos3.zip
│   │   ├── Estabelecimentos4.zip
│   │   ├── Estabelecimentos5.zip
│   │   ├── Estabelecimentos6.zip
│   │   ├── Estabelecimentos7.zip
│   │   ├── Estabelecimentos8.zip
│   │   ├── Estabelecimentos9.zip
│   │   ├── K3241.K03200Y9.D21119.EMPRECSV
│   │   ├── LAYOUT_DADOS_ABERTOS_CNPJ.pdf
│   │   ├── Motivos.zip
│   │   ├── Municipios.zip
│   │   ├── Naturezas.zip
│   │   ├── Paises.zip
│   │   ├── Qualificacoes.zip
│   │   ├── Simples.zip
│   │   ├── Socios0.zip
│   │   ├── Socios1.zip
│   │   ├── Socios2.zip
│   │   ├── Socios3.zip
│   │   ├── Socios4.zip
│   │   ├── Socios5.zip
│   │   ├── Socios6.zip
│   │   ├── Socios7.zip
│   │   ├── Socios8.zip
│   │   └── Socios9.zip
│   └── .DS_Store
├── 1-CEIS
│   ├── .DS_Store
│   ├── 20221111_CEIS.csv
│   ├── 20230223_CEIS.csv
│   ├── 20230223_CEIS.zip
│   └── CEIS 2022111.zip
├── 2-CEPIM
│   ├── 20221109_CEPIM.csv
│   ├── 20230228_CEPIM.csv
│   └── 20230228_CEPIM.zip
├── 3-PEP
│   ├── 202210_PEP.csv
│   └── 202212_PEP.csv
├── 4-CNEP
│   ├── 20230223_CNEP.csv
│   └── 20230223_CNEP.zip
├── 5-CEAF
│   └── 20230223_Expulsoes.csv
├── 6-AcordosLeniencia
│   ├── 20230223_Acordos.csv
│   └── 20230223_Efeitos.csv
├── 7-Benef Sociais
│   ├── 6.1-Auxilio Emergencial
│   │   ├── 202204_AuxilioEmergencial.csv
│   │   └── 202212_AuxilioEmergencial.zip
│   ├── 6.2-Seguro Defeso
│   │   ├── 202210_SeguroDefeso.csv
│   │   └── 202210_SeguroDefeso.zip
│   ├── 6.3-Benefício de Prestacao Continuada
│   │   ├── .DS_Store
│   │   ├── 202210_BPC.csv
│   │   └── 202210_BPC.zip
│   ├── 6.4-Auxilio Brasil
│   │   ├── .DS_Store
│   │   ├── 202211_AuxilioBrasil.csv
│   │   └── 202211_AuxilioBrasil.zip
│   ├── 6.5-Garantia Safra
│   │   ├── .DS_Store
│   │   ├── 202210_GarantiaSafra.csv
│   │   └── 202210_GarantiaSafra.zip
│   ├── 6.6-PETI
│   │   ├── .DS_Store
│   │   ├── 202205_PETI.csv
│   │   └── 202205_PETI.zip
│   └── .DS_Store
├── 8-Divida Ativa
│   ├── 8.1-Nao_Previdenciario
│   │   ├── arquivo_lai_SIDA_1_202212.csv
│   │   ├── arquivo_lai_SIDA_2_202212.csv
│   │   ├── arquivo_lai_SIDA_3_202212.csv
│   │   ├── arquivo_lai_SIDA_4_202212.csv
│   │   ├── arquivo_lai_SIDA_5_202212.csv
│   │   ├── arquivo_lai_SIDA_6_202212.csv
│   │   ├── Dados_abertos_Nao_Previdenciariozip.zip
│   │   └── sida.db
│   ├── 8.2-FGTS
│   │   ├── Dados_abertos_FGTS
│   │   │   ├── arquivo_lai_FGTS_-9_202212.csv
│   │   │   ├── arquivo_lai_FGTS_1_202212.csv
│   │   │   ├── arquivo_lai_FGTS_2_202212.csv
│   │   │   ├── arquivo_lai_FGTS_3_202212.csv
│   │   │   ├── arquivo_lai_FGTS_4_202212.csv
│   │   │   ├── arquivo_lai_FGTS_5_202212.csv
│   │   │   └── arquivo_lai_FGTS_6_202212.csv
│   │   ├── .DS_Store
│   │   └── Dados_abertos_FGTS.zip
│   ├── 8.3-Previdenciarios
│   │   ├── Dados_abertos_Previdenciario
│   │   │   ├── .DS_Store
│   │   │   ├── arquivo_lai_PREV_1_202212.csv
│   │   │   ├── arquivo_lai_PREV_2_202212.csv
│   │   │   ├── arquivo_lai_PREV_3_202212.csv
│   │   │   ├── arquivo_lai_PREV_4_202212.csv
│   │   │   ├── arquivo_lai_PREV_5_202212.csv
│   │   │   └── arquivo_lai_PREV_6_202212.csv
│   │   ├── .DS_Store
│   │   └── Dados_abertos_Previdenciario.zip
│   └── .DS_Store
├── Compranet 2022
│   ├── .project
│   ├── compras
│   ├── compras.db
│   ├── comprasnet-contratos-anual-contratos-latest.csv
│   ├── comprasnet-contratos-anual-cronogramas-latest.csv
│   ├── comprasnet-contratos-anual-despesas_acessorias-latest.csv
│   ├── comprasnet-contratos-anual-empenhos-latest.csv
│   ├── comprasnet-contratos-anual-faturas-latest.csv
│   ├── comprasnet-contratos-anual-garantias-latest.csv
│   ├── comprasnet-contratos-anual-historicos-latest.csv
│   ├── comprasnet-contratos-anual-itens-latest.csv
│   ├── comprasnet-contratos-anual-prepostos-latest.csv
│   ├── comprasnet-contratos-anual-responsaveis-latest.csv
│   ├── comprasnet-contratos-anual-terceirizados-latest.csv
│   └── dicionário-de-dados.odt
└── .DS_Store
````