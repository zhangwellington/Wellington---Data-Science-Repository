def triagem():
    # **Triagem dos Arquivos baixados**


    ## Inicialização

    import pandas as pd
    import numpy as np
    import os
    import datetime
    import time

    from google.colab import drive
    drive.mount('/content/drive')

    ##Apagar COLLECTI.ZIP se existir

    mydrive='/content/drive/MyDrive/'
    dir_carteiras=mydrive+'Carteiras/'
    #Remover COLLECTI.ZIP
    try:
        os.remove(mydrive+'COLLECTI.ZIP')
    except:
        print('Arquivo COLLECTI.ZIP não existente')
    #Remover SALDOS_C.ZIP
    try:
        os.remove(mydrive+'SALDOS_C.ZIP')
    except:
        print('Arquivo SALDOS_C.ZIP não existente')

    ##Importando Tabela de Diretórios

    xlsx_diretorios=pd.read_excel(mydrive+'Processamentos/Tratamento/auxiliary/Diretorios.xlsx',sheet_name='Arquivos',dtype=object)
    nomes=np.array(xlsx_diretorios['Nome']).astype(str)
    carteiras=np.array(xlsx_diretorios['Carteira'])
    diretorios=np.array(xlsx_diretorios['Diretório'])
    print(nomes)

    rodar=[]
    for arquivo in os.listdir(dir_carteiras):
        print(arquivo)
    for i in range(len(nomes)):
        if arquivo.find(nomes[i])>=0:
            rodar.append(carteiras[i])
            os.makedirs(diretorios[i],exist_ok=True)
            os.rename(dir_carteiras+arquivo,diretorios[i]+arquivo)
            continue

    ##Carteiras que tiveram arquivos movidos

    rodar=np.array(rodar)
    rodar=np.unique(rodar)
    rodar=np.sort(rodar)
    print(list(rodar),len(rodar))