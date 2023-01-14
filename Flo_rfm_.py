# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)

# İş Problemi (Business Problem)
"""
Online ayakkabı mağazası olan FLO müşterilerini segmentlere ayırıp bu segmentlere göre
pazarlama stratejileri belirlemek istiyor. Buna yönelik olarak müşterilerin davranışları
tanımlanacak ve bu davranışlardaki öbeklenmelere göre gruplar oluşturulacak.
"""

# Veri Seti Hikayesi
"""
Veri seti Flo'dan son alışverişlerini 2020-2021 yıllarında 
OmniChannel(hem online hem office alışveriş yapan) olarak yapan müşterilerin
geçmiş alışveriş davranışlarından elde edilen bilgilerden oluşmaktadır.

master_id: Eşsiz müşteri numarası
order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
last_order_channel : En son alışverişin yapıldığı kanal
first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
last_order_date : Müşterinin yaptığı son alışveriş tarihi
last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi
"""

# GÖREVLER
"""
GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
           1. flo_data_20K.csv verisini okuyunuz.
           2. Veri setinde
                     a. İlk 10 gözlem,
                     b. Değişken isimleri,
                     c. Betimsel istatistik,
                     d. Boş değer,
                     e. Değişken tipleri, incelemesi yapınız.
           3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
           alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
           4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
           5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.
           6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
           7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
           8. Veri ön hazırlık sürecini fonksiyonlaştırınız.
"""
#GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.set_option('display.width',1000)

# 1. flo_data_20K.csv verisini okuyunuz.
df_=pd.read_csv("C:/Users/User/Desktop/Git-Hub/crm_/Flo_customer_segmentation/flo_data_20K.csv")
df=df_.copy()
print(df.head())

# 2. Veri setinde
# a. İlk 10 gözlem,
print(df.head(10))
# b. Değişken isimleri,
print(df.columns)
# c. Betimsel istatistik,
print(df.shape)
# d. Boş değer,
print(df.isnull().sum())
# e. Değişken tipleri, incelemesi yapınız.
print(df.info())

# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir.
# Herbir müşterinin toplam alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
df["order_num_total"]=df["order_num_total_ever_online"]+df["order_num_total_ever_offline"]
df["customer_value_total"]=df["customer_value_total_ever_offline"]+df["customer_value_total_ever_online"]

#4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime)
print(df.info())

#5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.
df.groupby("order_channel").agg({"master_id":"count",
                                 "order_num_total":"sum",
                                 "customer_value_total":"sum"})
# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df.sort_values("customer_value_total", ascending=False)[:10]

# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.sort_values("order_num_total", ascending=False)[:10]

# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.
def data_prep(dataframe):
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    return df

# GÖREV 2: RFM Metriklerinin Hesaplanması
df["last_order_date"].max() # 2021-05-30
analysis_date = dt.datetime(2021,6,1)

# customer_id, recency, frequnecy ve monetary değerlerinin yer aldığı yeni bir rfm dataframe
rfm = pd.DataFrame()
rfm["customer_id"] = df["master_id"]
rfm["recency"] = (analysis_date - df["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = df["order_num_total"]
rfm["monetary"] = df["customer_value_total"]

print(rfm.head())

# GÖREV 3: RF ve RFM Skorlarının Hesaplanması (Calculating RF and RFM Scores)
#  Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çevrilmesi ve
# Bu skorları recency_score, frequency_score ve monetary_score olarak kaydedilmesi
rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm.head()


# recency_score ve frequency_score’u tek bir değişken olarak ifade edilmesi ve RF_SCORE olarak kaydedilmesi
rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))


# 3. recency_score ve frequency_score ve monetary_score'u tek bir değişken olarak ifade edilmesi ve RFM_SCORE olarak kaydedilmesi
rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str) + rfm['monetary_score'].astype(str))

print(rfm.head())

# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması
# Oluşturulan RFM skorların daha açıklanabilir olması için segment tanımlama ve  tanımlanan seg_map yardımı ile RF_SCORE'u segmentlere çevirme
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

print(rfm.head())

# GÖREV 5: Aksiyon zamanı!
# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

#                          recency       frequency       monetary
#                        mean count      mean count     mean count
# segment
# about_to_sleep       113.79  1629      2.40  1629   359.01  1629
# at_Risk              241.61  3131      4.47  3131   646.61  3131
# cant_loose           235.44  1200     10.70  1200  1474.47  1200
# champions             17.11  1932      8.93  1932  1406.63  1932
# hibernating          247.95  3604      2.39  3604   366.27  3604
# loyal_customers       82.59  3361      8.37  3361  1216.82  3361
# need_attention       113.83   823      3.73   823   562.14   823
# new_customers         17.92   680      2.00   680   339.96   680
# potential_loyalists   37.16  2938      3.30  2938   533.18  2938
# promising             58.92   647      2.00   647   335.67   647

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulunuz ve müşteri id'lerini csv ye kaydediniz.
# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
# tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Bu müşterilerin sadık  ve
# kadın kategorisinden alışveriş yapan kişiler olması planlandı. Müşterilerin id numaralarını csv dosyasına yeni_marka_hedef_müşteri_id.cvs
# olarak kaydediniz.

target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) &(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_ids.to_csv("yeni_marka_hedef_müşteri_id.csv", index=False)
print(cust_ids.shape)

print(rfm.head())

# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşterilerden olan ama uzun süredir
# alışveriş yapmayan ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
# olarak kaydediniz.
target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)



# BONUS


def create_rfm(dataframe):
    # Veriyi Hazırlma
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)


    # RFM METRIKLERININ HESAPLANMASI
    dataframe["last_order_date"].max()  # 2021-05-30
    analysis_date = dt.datetime(2021, 6, 1)
    rfm = pd.DataFrame()
    rfm["customer_id"] = dataframe["master_id"]
    rfm["recency"] = (analysis_date - dataframe["last_order_date"]).astype('timedelta64[D]')
    rfm["frequency"] = dataframe["order_num_total"]
    rfm["monetary"] = dataframe["customer_value_total"]

    # RF ve RFM SKORLARININ HESAPLANMASI
    rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])
    rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))
    rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str) + rfm['monetary_score'].astype(str))


    # SEGMENTLERIN ISIMLENDIRILMESI
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }
    rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

    return rfm[["customer_id", "recency","frequency","monetary","RF_SCORE","RFM_SCORE","segment"]]

rfm_df = create_rfm(df)
print(rfm_df.head(10))
"""
                            customer_id  recency  frequency  monetary RF_SCORE RFM_SCORE              segment
0  cc294636-19f0-11eb-8d74-000d3a38a36f    95.00       5.00    939.37       34       344      loyal_customers
1  f431bd5a-ab7b-11e9-a2fc-000d3a38a36f   105.00      21.00   2013.55       35       355      loyal_customers
2  69b69676-1a40-11ea-941b-000d3a38a36f   186.00       5.00    585.32       24       243              at_Risk
3  1854e56c-491f-11eb-806e-000d3a38a36f   135.00       2.00    121.97       31       311       about_to_sleep
4  d6ea1074-f1f5-11e9-9346-000d3a38a36f    86.00       2.00    209.98       31       311       about_to_sleep
5  e585280e-aae1-11e9-a2fc-000d3a38a36f    80.00       3.00    200.86       42       421  potential_loyalists
6  c445e4ee-6242-11ea-9d1a-000d3a38a36f   226.00       4.00    375.93       23       232              at_Risk
7  3f1b4dc8-8a7d-11ea-8ec0-000d3a38a36f   293.00       2.00    163.63       11       111          hibernating
8  cfbda69e-5b4f-11ea-aca7-000d3a38a36f    86.00       5.00   1054.69       34       345      loyal_customers
9  1143f032-440d-11ea-8b43-000d3a38a36f   240.00       2.00    165.96       11       111          hibernating
"""