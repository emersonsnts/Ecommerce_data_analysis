'''SATISFACTION ANALYSIS, CUSTOMER EVALUATION AND DELIVERY TIME'''

import pyodbc
import pandas as pd
import warnings
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud, ImageColorGenerator

#%%ERROR HANDLING
warnings.filterwarnings('ignore')


#%%QUERY FUNCTION DEFINITION IN SQL
def Query(query):
    return pd.read_sql(query, conexãoDB)


#%%DATABASE CONNECTION
conexãoDB=pyodbc.connect(
    DRIVER='ODBC Driver 17 for SQL Server',
    SERVER='EMERSONPC',
    DATABASE='DataScience',
    Trusted_connection='yes'
)
cursor=conexãoDB.cursor()


#%%DATA QUERY
#df_o_r means Order Review Dataframe
df_r_o=Query("""
SELECT order_purchase_timestamp, order_approved_at, order_status, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date, review_score, review_comment_message 
FROM Orders LEFT JOIN Order_Reviews on Orders.order_id = Order_Reviews.order_id 
""")


#%%DATA CLEANING
df_r_o=df_r_o.replace(['nan'], [np.nan])
df_r_o=df_r_o[df_r_o.order_status=='delivered']
columns=list(df_r_o.columns[:2]) + list(df_r_o.columns[3:6])
for i in columns:
    df_r_o=df_r_o[df_r_o[i].notna()]
df_r_o['review_score']=df_r_o['review_score'].astype(int)
df_r_o=df_r_o[(df_r_o['review_score']>=1) & (df_r_o['review_score']<=5)].reset_index(drop=True)


#%%DATA PROCESSING
date_columns=list(df_r_o.columns[:2]) + list(df_r_o.columns[3:6])
for i in date_columns:
    df_r_o[i]=pd.to_datetime(df_r_o[i])
df_r_o['delivery_time_indicator_day']=df_r_o['order_estimated_delivery_date'] - df_r_o['order_delivered_customer_date']
df_r_o['delivery_time_indicator_day']=df_r_o['delivery_time_indicator_day'].apply(lambda x: int(str(x).split(' ')[0]))


#%%FIGURE CREATION FOR GRAPHIC PLOT
plt.style.use('seaborn-darkgrid')
fig, ax=plt.subplots(nrows=3, ncols=2, figsize=(16,9))
plt.tight_layout()
palette = sns.color_palette('muted')

#%%REVIEW SCORE FREQUENCY DISTRIBUTION ANALYSIS
ax[0,0]=sns.histplot(data=df_r_o, x=df_r_o['review_score'])
ax[0,0].set_xticks([1,2,3,4,5])
ax[0,0].set_title('Figure 1: Review Score Frequency Distribution of 100 Thousand Orders', fontsize=14, weight='bold')
ax[0,0].set_xlabel('Rating Score', fontsize=11)
ax[0,0].set_ylabel('Number of Orders', fontsize=11)
ax[0,0].grid(visible=None)

colors_list=['red', 'darkorange', 'gold', 'darkkhaki', 'greenyellow']
count=0
for rectangle in ax[0,0].patches:
    if rectangle.get_height()!=0:
        print(rectangle.get_x())
        ax[0,0].text(rectangle.get_x()-0.03,
                 rectangle.get_height()+500,
                 f'{rectangle.get_height()/len(df_r_o):,.0%}',
                 weight='bold',
                 fontsize=12)
        rectangle.set_facecolor(f'{colors_list[count]}')
        count+=1

#%%WORDCLOUD DISPLAY OF THE MOST FREQUENT WORDS OF REVIEW SCORE BELOW 4
below3_reviews=' '.join(str(review) for review in df_r_o[df_r_o['review_comment_message'].notna()][df_r_o['review_score']<=3]['review_comment_message'])
stopwords_list=['ma']
wordcloud=WordCloud(stopwords=stopwords_list)
wordcloud.generate(below3_reviews)
ax[0,1].imshow(wordcloud, interpolation='bilinear')
ax[0,1].set_title('Figure 2: WordCloud of the most frequent words in Review Comments', fontsize=14, weight='bold')
ax[0,1].axis('off')


#%%CORRELATION ANALYSIS BETWEEN DELIVERY TIME AND AVERAGE RATING

#Creation of frequency distribution tables
class_table=[-12, -9, -6, -3, 0, 3, 6, 9, 12]
mean_table=[]
for i in range(len(class_table)):
    if i==0:
        mean_table.append(np.mean(df_r_o.query(f'delivery_time_indicator_day<{class_table[i]}')['review_score']))
    elif i==(len(class_table)-1):
        mean_table.append(np.mean(df_r_o.query(f'delivery_time_indicator_day>{class_table[i]}')['review_score']))
    else:
        mean_table.append(np.mean(df_r_o.query(f'delivery_time_indicator_day>{class_table[i]} and delivery_time_indicator_day<{class_table[i+1]} ')['review_score']))
df_freq_dist=pd.DataFrame(mean_table, index=class_table, columns=['review_score_mean'])

#Bar graph visualization
ax[1,0].bar(list(df_freq_dist.index), df_freq_dist['review_score_mean'])
ax[1,0].set_xticks(list(df_freq_dist.index))
ax[1,0].set_yticks([1,2,3,4,5])
for rectangle in ax[1,0].patches:
    ax[1,0].text(rectangle.get_x(),
             rectangle.get_height()+0.3,
             np.around(rectangle.get_height(),1)
             )
ax[1,0].set_title('Figure 3: Analysis of the association between Delay/Early Delivery Time and Average Rating Score', fontsize=11, weight='bold')
ax[1,0].set_xlabel('Delay/Early of Delivery Time (days)', fontsize=11)
ax[1,0].set_ylabel('Average Rating Score', fontsize=11)


#%%GRAPH PLOTTING
fig.set_dpi(200)
plt.subplots_adjust(wspace=0.1, bottom=0.05, hspace=0.2)
plt.savefig(r'E:\Data Science\Imagens\imgvarejo12.png', dpi=200)
plt.show()

#%%ANALYSIS OF PROCESSING TIMES FOR DELAYED ORDERS
#Creating the processing_time and transport_time indicators
df_r_o['processing_time']=df_r_o['order_delivered_carrier_date'] - df_r_o['order_approved_at']
df_r_o['processing_time']=df_r_o['processing_time'].apply(lambda x: int(str(x).split(' ')[0]))
df_r_o['processing_time_classification']=df_r_o['processing_time'].apply(lambda x: 'Processing within 24 hours' if x==0 else 'Processing after 24h')
df_o_delayed=df_r_o.query('delivery_time_indicator_day<0')

quantile_90=df_o_delayed['processing_time'].quantile(0.95)
sns.set_style('white')
sns.histplot(data=df_o_delayed.query(f'processing_time<{quantile_90} & processing_time>=0'), x='processing_time', hue='processing_time_classification', discrete=True, palette=['red', 'green'])
plt.xticks(range(0,max(df_o_delayed.query(f'processing_time<{quantile_90}')['processing_time'])+1))
plt.legend(['Processing within 24h', 'Processing after 24h'])
plt.title('Figure 4: Frequency Distribution and Proportion of Processing Times for Late Orders', fontsize=11, weight='bold')
plt.xlabel('Processing Time (days)')
plt.ylabel('Number of Orders')
plt.grid(False)
plt.savefig(r'E:\Data Science\Imagens\imgvarejo13.png', dpi=200)
plt.show()

orders_p_t_total=len(df_o_delayed['processing_time'])
orders_p_t_be1=len(df_o_delayed.query('processing_time<1'))
orders_p_t_ab1=len(df_o_delayed.query('processing_time>=1'))
plt.pie([orders_p_t_be1/orders_p_t_total,orders_p_t_ab1/orders_p_t_total ], autopct='%1.0f%%', colors=[palette[2], palette[3]], textprops={'fontsize':16}, startangle=0)
plt.savefig(r'E:\Data Science\Imagens\imgvarejo14.png', dpi=200)
plt.show()

#%%DELIVERY TIME ANALYSIS OF LATE ORDERS
df_r_o['transport_time']=df_r_o['order_delivered_customer_date'] - df_r_o['order_delivered_carrier_date']
df_r_o['transport_time']=df_r_o['transport_time'].apply(lambda x: int(str(x).split(' ')[0]))
df_r_o['carrier_estimative_transport_time']=df_r_o['order_estimated_delivery_date'] - df_r_o['order_approved_at']
df_r_o['carrier_estimative_transport_time']=df_r_o['carrier_estimative_transport_time'].apply(lambda x: (int(str(x).split(' ')[0])-1))
df_r_o['carrier_delivery_time']=df_r_o['carrier_estimative_transport_time']-df_r_o['transport_time']
df_r_o['carrier_delivery_time_classification']=df_r_o['carrier_delivery_time'].apply(lambda x: 'Carrier Complied with the Stipulations' if x>=0 else 'Carrier did not Comply with the Stipulations')

df_o_delayed=df_r_o.query('delivery_time_indicator_day<0')

quantile_95=df_o_delayed['carrier_delivery_time'].quantile(0.95)
quantile_5=df_o_delayed['carrier_delivery_time'].quantile(0.05)
sns.histplot(data=df_o_delayed.query(f'carrier_delivery_time<{quantile_95} & carrier_delivery_time>{quantile_5}'), x='carrier_delivery_time', discrete=True, hue='carrier_delivery_time_classification', palette=['green', 'red'])
plt.legend(['Carrier did not Comply with the Stipulations', 'Carrier Complied with the Stipulations'])
plt.title('Figure 5: Frequency Distribution and Proportion of Delay/Advance of Transport Time', fontsize=11, weight='bold')
plt.xlabel('Delay/Advance of Transport Time (days)')
plt.ylabel('Number of Orders')
plt.grid(False)
plt.savefig(r'E:\Data Science\Imagens\imgvarejo16.png', dpi=200)
plt.show()

orders_t_t_total=len(df_o_delayed['carrier_delivery_time'])
orders_t_t_be0=len(df_o_delayed.query('carrier_delivery_time<0'))
orders_t_t_ab0=len(df_o_delayed.query('carrier_delivery_time>=0'))
plt.pie([orders_t_t_ab0/orders_t_t_total,orders_t_t_be0/orders_t_t_total], autopct='%1.0f%%', colors=[palette[2], palette[3]], textprops={'fontsize':16}, startangle=0)
plt.savefig(r'E:\Data Science\Imagens\imgvarejo17.png', dpi=200)
plt.show()

#%%PARETO ANALYSIS

total_num_orders_late_delivery=len(df_o_delayed)
#Analysis of solving the processing time problem
processing_time_resolution=len(df_o_delayed.query('processing_time-1>= delivery_time_indicator_day*(-1)'))

#Analysis of resolving the delivery time problem
c_less_than_i=len(df_o_delayed.query('carrier_delivery_time<0 & carrier_delivery_time <= delivery_time_indicator_day'))

#Creation of frequency distribution table
pareto_table=pd.DataFrame([['Factor 1: Processing Time', processing_time_resolution, processing_time_resolution*100/total_num_orders_late_delivery, processing_time_resolution*100/total_num_orders_late_delivery], ['Factor 2: Transport Time', c_less_than_i, c_less_than_i*100/total_num_orders_late_delivery, 100]], columns=['Type', 'Number of Orders Delivered Late', 'Absolute Frequency', 'Acumulative Frequency'])
pareto_table=pareto_table.sort_values(by='Number of Orders Delivered Late', ascending=False)

#Creation of the Pareto Chart
plt.style.use('seaborn-darkgrid')
plt.figure(figsize=(16,9))
ax1 = sns.barplot(x=pareto_table['Type'], y=pareto_table['Number of Orders Delivered Late'])
ax1.set_title('Figure 6: Effects of Adjusting the Processing Time and the Transportation Time on Late Delivery Orders', fontsize=18, weight='bold')
ax1.set_ylim([0, int(processing_time_resolution)+500])
ax1.set_xlabel(' ')
ax1.set_ylabel(' ')
plt.xticks(fontsize=16)
plt.yticks(fontsize=14)

ax2 = ax1.twinx()
ax2.plot(pareto_table['Type'], pareto_table['Acumulative Frequency'], color=palette[3], marker='s', ms=7, label='Pareto')
ax2.tick_params(axis='y', colors=palette[3])
ax2.set_ylim([0,110])
plt.yticks(fontsize=14)


for i in range(len(pareto_table['Acumulative Frequency'])):
    ax2.text(pareto_table['Type'][i], pareto_table['Acumulative Frequency'][i] + 5, '%.2f' % pareto_table['Acumulative Frequency'][i] + '%', weight='bold', fontsize=14)
ax2.text(pareto_table['Type'][1], pareto_table['Absolute Frequency'][i] + 5, '%.2f' % pareto_table['Absolute Frequency'][1] + '%', weight='bold', fontsize=14)
plt.savefig(r'E:\Data Science\Imagens\imgvarejo18.png', dpi=200)
plt.show()