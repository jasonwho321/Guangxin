import requests

def worker(procnum, returns):
    '''worker function'''
    print(str(procnum) + ' represent!')
    returns.append(procnum)
    return returns

if __name__ == '__main__':
    print(requests.session().get('https://www.overstock.com/Home-Garden/FurnitureR-Dining-Table-Metal-Frame-Glass-Table-Top-White/28226797/product.html'))