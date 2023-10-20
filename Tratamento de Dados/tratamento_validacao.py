#**Listar arquivos da pasta Carteiras (tem validação de número de arquivos em Carteiras)**
def validacao():
    import os
    import sys
    import datetime

    from google.colab import drive
    drive.mount('/content/drive')

    diretorio='/content/drive/MyDrive/Carteiras/'
    today=datetime.date.today()
    os.listdir(diretorio)

    if today.weekday()==0:
        if len(os.listdir(diretorio))!=14:
            print('Não tem 14 arquivos em Carteiras')
            sys.exit()
    else:
        if len(os.listdir(diretorio))!=9:
            print('Não tem 9 arquivos em Carteiras')
            sys.exit()