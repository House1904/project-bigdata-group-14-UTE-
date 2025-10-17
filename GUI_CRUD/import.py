import happybase
import csv

# Kết nối tới HBase Thrift server
conn = happybase.Connection('localhost')
table = conn.table('anilist_db')

# Mở file CSV
with open('anilist_cleaned.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)

    for row in reader:
        # Ghi từng bản ghi vào HBase (rowkey = id)
        table.put(row['id'].encode(), {
            b'info:title_english': row['title_english'].encode(),
            b'info:duration': row['duration'].encode(),
            b'info:score': row['score'].encode(),
            b'info:genres': row['genres'].encode(),
            b'info:studio': row['studio'].encode(),
            b'info:start_year': row['start_year'].encode(),
            b'detail:title_romaji': row['title_romaji'].encode(),
            b'detail:title_native': row['title_native'].encode(),
            b'detail:format': row['format'].encode(),
            b'detail:episodes': row['episodes'].encode(),
            b'detail:source': row['source'].encode(),
            b'detail:country': row['country'].encode(),
            b'detail:isAdult': row['isAdult'].encode(),
            b'social:meanScore': row['meanScore'].encode(),
            b'social:popularity': row['popularity'].encode(),
            b'social:favourites': row['favourites'].encode(),
            b'social:trending': row['trending'].encode(),
            b'social:tags': row['tags'].encode(),
            b'social:status': row['status'].encode(),
            b'social:season': row['season'].encode(),
            b'social:url': row['url'].encode()
        })
conn.close()
