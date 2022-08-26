import pip._internal

if __name__=='__main__':
    with open('env.txt',encoding='utf-8') as f:
        for package in f:
            pip._internal.main(['install', package])