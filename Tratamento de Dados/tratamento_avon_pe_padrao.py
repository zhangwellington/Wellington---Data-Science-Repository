def tratamento():
	# **Tratamento AVON PE PADRAO (sem Collecti)**
	
	## Inicialização
	
	#ATUALIZAR A LISTA DE DATAS QUE INICIAM AS CAMPANHAS
	mover=True
	tem_descuentos=False
	
	import pandas as pd
	import numpy as np
	import os
	import datetime
	import time
	
	###Verificar se tem Descuentos
	
	if datetime.date.today()<=datetime.date(2023,9,30):
	  tem_descuentos=True
	
	from google.colab import drive
	drive.mount('/content/drive')
	
	carteira='Avon PE'
	
	today=datetime.date.today()
	#Definindo diretórios
	diretorio_tratamento=r'/content/drive/MyDrive/Processamentos/Tratamento/'
	diretorio=diretorio_tratamento + carteira + '/'
	output='/'.join(['/content/drive/MyDrive/Pronto pra upload',str(today.year),str(today.month).zfill(2),today.strftime('%d-%m-%Y')])+'/'
	os.makedirs(output,exist_ok=True)
	ingestion=diretorio+'ingestion/'
	processedfiles=diretorio+'processedfiles/'
	dir_descuentos=diretorio+'Descuentos/'
	auxiliary=diretorio+'auxiliary/'
	
	#Nome do arquivo de saída com caminho
	csv_saida=output+'early_avon-pe-padrao_'+str(today.strftime('%d-%m-%Y'))+'.csv'
	
	##Identificar arquivos da auxiliary
	
	#Emails e Calendario
	for arquivo in os.listdir(auxiliary):
	    if arquivo.find('Emails')>=0:
	        arquivo_emails=auxiliary+arquivo
	        nome_emails=arquivo
	    elif arquivo.find('Calendario')>=0:
	        arquivo_calendario=auxiliary+arquivo
	        nome_calendario=arquivo
	
	##Importar arquivo Descuentos
	
	if tem_descuentos:
	  #Identificando o arquivo mais recente da pasta Descuentos
	  df_arquivo_recente=pd.DataFrame({'Nome':os.listdir(dir_descuentos)})
	  tempo_modif=np.empty(len(df_arquivo_recente))
	  i=0
	  for arquivo in os.listdir(dir_descuentos):
	    tempo_modif[i]=os.path.getmtime(dir_descuentos+arquivo)
	    i+=1
	  df_arquivo_recente['Tempo de Modificacao']=tempo_modif
	  df_arquivo_recente=df_arquivo_recente.sort_values(by='Tempo de Modificacao',ascending=False)
	  df_arquivo_recente.index=range(len(df_arquivo_recente))
	  print(df_arquivo_recente)
	  nome_arquivo_descuentos=str(df_arquivo_recente['Nome'][0])
	  arquivo_descuentos=dir_descuentos+nome_arquivo_descuentos
	  print(arquivo_descuentos)
	
	##Importar arquivo de Calendário
	
	df_calendario=pd.read_excel(arquivo_calendario,dtype=object)
	#Definir último dia
	if today.weekday()==0:
	  dias=3
	else:
	  dias=1
	ultimo=today-datetime.timedelta(days=dias)
	
	##Definir se Tem Collecti
	
	#Checando se tem collecti
	if str(ultimo) in np.array(df_calendario['Data'].astype(str)):
	  tem_collecti=True
	  zona=np.array(df_calendario.loc[df_calendario['Data']==str(ultimo),'Zona']).astype(int)
	else:
	  tem_collecti=False
	print(tem_collecti)
	tem_collecti=False
	
	##Ingestion
	
	#Esperando ler 1 ou 2 arquivos na ingestion (depende do parâmetro 'tem_especial')
	if tem_collecti:
	  num_arquivo=2
	else:
	  num_arquivo=1
	
	while len(os.listdir(ingestion))!=num_arquivo:
	  time.sleep(5)
	print(os.listdir(ingestion))
	
	##Identificar arquivos da ingestion
	
	#Identificando arquivo COLLECTI e SALDOS
	for arquivo in os.listdir(ingestion):
	    if arquivo.find('COLLECTI')>=0:
	        arquivo_txt=ingestion+arquivo
	        nome_txt=arquivo
	    elif arquivo.find('SALDOS_C')>=0:
	        arquivo_saldos=ingestion+arquivo
	        nome_saldos=arquivo
	
	##Importando Arquivos
	
	#Carregando Planilha Collecti
	if tem_collecti:
	  df_txt=pd.read_csv(arquivo_txt,dtype=object,sep='\t', lineterminator='\r',encoding='unicode_escape',encoding_errors='ignore')
	#Carregando Planilha Saldos
	df_saldos=pd.read_csv(arquivo_saldos,dtype=object,sep='\t', lineterminator='\r',encoding='unicode_escape',encoding_errors='ignore')
	#Carregando planilha de descontos se tiver
	if tem_descuentos:
	  df_descuentos=pd.read_excel(arquivo_descuentos,dtype=object)
	#Carregando planilha de Emails
	df_emails=pd.read_csv(arquivo_emails,dtype=object)
	
	##Início das Tranformações
	
	###Juntar tabela de Saldos e Collecti
	
	####Criando cópias
	
	#Criando cópia do txt para tratar
	if tem_collecti:
	  txt=df_txt.copy()
	saldos=df_saldos.copy()
	emails=df_emails.copy()
	if tem_descuentos:
	  descuentos=df_descuentos.copy()
	
	###Atualizar e Exportar planilha de Emails com o arquivo COLLECTI novo
	
	#Tratando coluna ZONA
	if tem_collecti:
	  txt['ZONA']=txt['ZONA'].str.replace('\n','').str.strip()
	  emails['ZONA']=emails['ZONA'].str.replace('\n','').str.strip()
	  #Validação das ZONAs
	  txt_zonas=txt.loc[txt['ZONA']!='','ZONA'].unique().astype(int)  #zona do calendário está em int tbm
	  for txt_zona in txt_zonas:
	    if txt_zona not in zona:
	      print('ZONA do Calendário não está no COLLECTI')
	      import sys
	      sys.exit()
	  for x in zona:
	    if not x in txt_zonas:
	      print('ZONA do COLLECTI não está no Calendário')
	      sys.exit()
	
	if tem_collecti:
	  #Retirar Emails do Collecti novo do arquivo antigo de Emails
	  filtro=np.logical_not(np.isin(emails.ZONA.astype(int),zona))  #transformar em int tira os epaços antes
	  emails=emails[filtro]
	  #Agrupar emails por external ID
	  novo=pd.concat([emails,txt])
	  print('Antes',len(novo))
	  novo=novo.groupby(['ZONA','CARTA','SEC','EQUIPO','TERRITORIO'],as_index=False).last()
	  print('Depois',len(novo))
	
	###Selecionar colunas da Tabela de Emails
	
	emails=emails[['ZONA','CARTA','SEC','EQUIPO','TERRITORIO','E-MAIL']]
	
	####Tratando coluna de Zonas
	
	#Tratando ZONA
	saldos['ZONA']=saldos['ZONA'].str.replace('\n','').str.strip()
	
	####Eliminando ZONAS da tabela SALDOS que estão na COLLECTI
	
	if tem_collecti:
	  filtro=np.logical_not(np.isin(saldos.ZONA,np.unique(txt.ZONA)))
	  saldos=saldos[filtro]
	  print(len(saldos)-filtro.sum(),'linhas excluídas de',len(saldos))
	
	if tem_collecti:
	  saldos=pd.concat([txt,saldos],join='inner')
	
	###Tratando tabela de Descuentos se houver e Substituindo Deudas com Desconto
	
	if tem_descuentos:
	  #Deletando colunas indesejadas, agrupando por TERRITORIO e pegando o valor com menor desconto se houver duplicado
	  coluna_importe='Monto a cancelar hasta el 30/09'
	  colunas=['TERRITORIO',coluna_importe]
	  descuentos=descuentos.sort_values('% DESCUENTO').groupby('TERRITORIO',as_index=False).first()[colunas]
	  descuentos['TERRITORIO']=descuentos['TERRITORIO'].astype(str)
	  #Merge com tabela principal
	  saldos=saldos.merge(descuentos,on='TERRITORIO',how='left')
	  #Renomeando coluna Importe por DEUDA A PAGAR
	  saldos=saldos.rename(columns={coluna_importe:'DEUDA A PAGAR'})
	  #Preenchendo DEUDA A PAGAR vazios POR DEU+PER
	  filtro=saldos['DEUDA A PAGAR'].isna()
	  saldos.loc[filtro,'DEUDA A PAGAR']=saldos.loc[filtro,'DEU+PER']
	else:
	  saldos['DEUDA A PAGAR']=saldos['DEU+PER']
	
	###Tratamento da Carteira
	
	####Apagar linhas nulas
	
	#Apagando linhas nulas
	saldos.dropna(subset='TERRITORIO',inplace=True)
	
	####Aplicando strip em todas as colunas
	
	#Aplicando strip em todas as colunas e substituindo } por '' e substituindo nan por ''
	for coluna in saldos.columns.to_list():
	  saldos[coluna]=saldos[coluna].astype(str)
	  saldos[coluna]=saldos[coluna].str.strip().str.replace('}','')
	  saldos[coluna]=saldos[coluna].str.replace('nan','')
	
	####Formatando as colunas numéricas apra 2 casas
	
	#Formatando colunas numéricas para 2 casas decimais
	colunas=['DEUDA INICIAL','DEUDA','DEU+PER','DEUDA A PAGAR']
	for coluna in colunas:
	  #Transformar vazios em 0
	  saldos[coluna]=saldos[coluna].apply(lambda x: '0' if str(x)=='' else x)
	  #Se último caractere for letra, retirar ele
	  saldos[coluna]=saldos[coluna].apply(lambda x: str(x)[:-1] if str(x)[-1].isalpha() else str(x))
	  #Formatando em número de 2 casas decimais
	  saldos[coluna]=pd.to_numeric(saldos[coluna])
	  saldos[coluna].fillna(0,inplace=True)
	  saldos[coluna]=saldos[coluna].map('{:.2f}'.format)
	  saldos[coluna]=saldos[coluna].astype(object)
	
	###Tratamento de Número de Celular
	
	import re
	colunas=['TELEFONO 1', 'TELEFONO 2', 'TELEFONO 3', 'TELEFONO 4']
	#Tirar espaços
	for coluna in colunas:
	  #Tirar caracteres não numéricos
	  saldos[coluna]=saldos[coluna].apply(lambda x: re.sub('[^0-9]', '', str(x)))
	  #Tirar números com mais de 12 dígitos ou menos de 8 dígitos
	  saldos[coluna]=saldos[coluna].apply(lambda x: '' if len(x)>12 or len(x)<8 else x)
	  #Pegar últimos 9 dígitos de números com mais de 9 dígitos e completar com dígito 9
	  saldos[coluna]=saldos[coluna].apply(lambda x: x[-9:] if len(x)>9 else ('9'+ x if len(x)==8 else x))
	  #Apagar números que não começam com 9
	  filtro=saldos[coluna]!=''
	  saldos.loc[filtro,coluna]=saldos.loc[filtro,coluna].apply(lambda x: '' if x[0]!='9' else x)
	#Completar TELEFONO 1 nulos pelo 2, 3 e 4
	filtro=saldos['TELEFONO 1']==''
	saldos.loc[filtro,'TELEFONO 1']=saldos.loc[filtro,'TELEFONO 2']
	filtro=saldos['TELEFONO 1']==''
	saldos.loc[filtro,'TELEFONO 1']=saldos.loc[filtro,'TELEFONO 3']
	filtro=saldos['TELEFONO 1']==''
	saldos.loc[filtro,'TELEFONO 1']=saldos.loc[filtro,'TELEFONO 4']
	
	###Tratando as colunas ZONA, CARTA, SEC, EQUIPO, TERRITORIO de emails e saldos
	
	colunas=['ZONA', 'CARTA', 'SEC', 'EQUIPO', 'TERRITORIO']
	for coluna in colunas:
	    saldos[coluna]=saldos[coluna].astype(str).str.strip()
	    emails[coluna]=emails[coluna].astype(str).str.strip()
	saldos['EXTERNAL']=saldos['ZONA']+' '+saldos['CARTA']+' '+saldos['SEC']+' '+saldos['EQUIPO']+' '+saldos['TERRITORIO']
	saldos['EXTERNAL']=saldos['EXTERNAL'].astype(str).str.strip()
	emails['EXTERNAL']=emails['ZONA']+' '+emails['CARTA']+' '+emails['SEC']+' '+emails['EQUIPO']+' '+emails['TERRITORIO']
	emails['EXTERNAL']=emails['EXTERNAL'].astype(str).str.strip()
	emails.drop(colunas,axis=1,inplace=True)
	
	###Mesclando coluna de Email
	
	print('Antes',len(saldos))
	saldos=saldos.merge(emails,on='EXTERNAL',how='left')
	print('Depois',len(saldos))
	
	###Tratamento de Email
	
	#Aplicando lower na coluna de email
	coluna_email='E-MAIL'
	saldos[coluna_email]=saldos[coluna_email].str.lower().str.strip()
	#Apagando emails não validados
	saldos[coluna_email].loc[saldos[coluna_email].str.find('@')<0]=''
	saldos['num @']=saldos[coluna_email].str.len()-saldos[coluna_email].str.replace('@','').str.len()
	filtro=saldos['num @']!=1
	saldos.loc[filtro,coluna_email]=''
	saldos.drop('num @',axis=1,inplace=True)
	
	###Preenchendo ULT.A#O FACTURACION Vazio
	
	
	saldos.loc[saldos['ULT.A#O FACTURACION']=='','ULT.A#O FACTURACION']='2018'
	
	###Tratar coluna de Nome
	
	coluna_nomes=['APELLIDOS','NOMBRES']
	for coluna_nome in coluna_nomes:
	  saldos[coluna_nome]=saldos[coluna_nome].str.strip().str.title()
	
	##Exportando
	
	#Carteira Tratada
	saldos.to_csv(csv_saida,index=False)
	if tem_collecti:
	  #Arquivo de Emails.csv
	  novo.to_csv(arquivo_emails,index=False)
	
	##Movendo Arquivos da Ingestion
	
	if mover:
	  if tem_collecti:
	    arquivos=[nome_txt,nome_saldos]
	  else:
	     arquivos=[nome_saldos]
	  for arquivo in arquivos:
	    try:
	      os.rename(ingestion + arquivo, processedfiles + arquivo)
	    except:
	      os.remove(processedfiles + arquivo)
	      os.rename(ingestion + arquivo, processedfiles + arquivo)