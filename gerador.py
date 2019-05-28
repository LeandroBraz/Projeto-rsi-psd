import csv
def lercsv(nome_arq, lista):
    arq = open(nome_arq + '.csv')
    lin = csv.DictReader(arq)
    
    for i in lin:
        info = (i['Info']).split(',')
        lista.append(i['Source'] + ', ' + i['Time'] + ',' + info[-1])

    arq.close()
 

lista = []
lercsv('pcap0', lista)

lercsv('pcap1', lista)

lercsv('pcap2', lista)

lercsv('pcap3', lista)

lercsv('pcap4', lista)

lercsv('pcap5', lista)

lercsv('pcap6', lista)
print(len(lista))
'''for i in lista:
    print(i)'''
