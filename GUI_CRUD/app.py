from flask import Flask, render_template, request, redirect, url_for, jsonify
import happybase
import math
from datetime import datetime
import subprocess

app = Flask(__name__)

TABLE_NAME = 'anilist_db'
PAGE_SIZE = 10
SNAPSHOT_TABLE = 'snapshot_history'

def get_connection():
    return happybase.Connection(host='localhost', port=9090)

@app.route('/')
def index():
    page = int(request.args.get('page', 1))
    search = request.args.get('q', '').lower()

    conn = get_connection()
    table = conn.table(TABLE_NAME)

    all_rows = []
    for key, data in table.scan():
        # Lọc theo search trên info:title_english
        title = data.get(b'info:title_english', b'').decode()
        if search and search not in title.lower():
            continue

        row = {
            'id': (data.get(b'info:id') or key).decode(),
            'title_english': data.get(b'info:title_english', b'').decode(),
            'duration': data.get(b'info:duration', b'').decode(),
            'score': data.get(b'info:score', b'').decode(),
            'genres': data.get(b'info:genres', b'').decode(),
            'studio': data.get(b'info:studio', b'').decode(),
            'start_year': data.get(b'info:start_year', b'').decode(),
        }

        all_rows.append(row)

    total = len(all_rows)
    total_pages = math.ceil(total / PAGE_SIZE)
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    rows = all_rows[start:end]

    conn.close()
    return render_template('index.html', animes=rows, page=page, total_pages=total_pages, search=search)

@app.route('/detail/<id>')
def detail(id):
    conn = get_connection()
    table = conn.table(TABLE_NAME)
    row_data = table.row(id.encode())

    detail = {
        'title_romaji': row_data.get(b'detail:title_romaji', b'').decode(),
        'title_native': row_data.get(b'detail:title_native', b'').decode(),
        'format': row_data.get(b'detail:format', b'').decode(),
        'episodes': row_data.get(b'detail:episodes', b'').decode(),
        'source': row_data.get(b'detail:source', b'').decode(),
        'country': row_data.get(b'detail:country', b'').decode(),
        'isAdult': row_data.get(b'detail:isAdult', b'').decode(),
        'meanScore': row_data.get(b'social:meanScore', b'').decode(),
        'popularity': row_data.get(b'social:popularity', b'').decode(),
        'favorites': row_data.get(b'social:favorites', b'').decode(),
        'trending': row_data.get(b'social:trending', b'').decode(),
        'tags': row_data.get(b'social:tags', b'').decode(),
        'status': row_data.get(b'social:status', b'').decode(),
        'season': row_data.get(b'social:season', b'').decode(),
        'url': row_data.get(b'social:url', b'').decode(),
    }

    conn.close()
    return render_template('detail.html', id=id, detail=detail)

@app.route('/add', methods=['GET', 'POST'])
def add_anime():
    if request.method == 'POST':
        data = request.form
        conn = get_connection()
        table = conn.table(TABLE_NAME)
        table.put(data['id'].encode(), {
            # info
            b'info:id': data['id'].encode(),
            b'info:title_english': data.get('title_english','').encode(),
            b'info:duration': data.get('duration','').encode(),
            b'info:score': data.get('score','').encode(),
            b'info:genres': data.get('genres','').encode(),
            b'info:studio': data.get('studio','').encode(),
            b'info:start_year': data.get('start_year','').encode(),
            # detail
            b'detail:title_romaji': data.get('title_romaji','').encode(),
            b'detail:title_native': data.get('title_native','').encode(),
            b'detail:format': data.get('format','').encode(),
            b'detail:episodes': data.get('episodes','').encode(),
            b'detail:source': data.get('source','').encode(),
            b'detail:country': data.get('country','').encode(),
            b'detail:isAdult': data.get('isAdult','').encode(),
            # social
            b'social:meanScore': data.get('meanScore','').encode(),
            b'social:popularity': data.get('popularity','').encode(),
            b'social:favorites': data.get('favorites','').encode(),
            b'social:trending': data.get('trending','').encode(),
            b'social:tags': data.get('tags','').encode(),
            b'social:status': data.get('status','').encode(),
            b'social:season': data.get('season','').encode(),
            b'social:url': data.get('url','').encode(),
        })
        conn.close()
        return redirect(url_for('index'))

    return render_template('add.html')

@app.route('/delete/<id>', methods=['POST'])
def delete_anime(id):
    conn = get_connection()
    table = conn.table(TABLE_NAME)
    table.delete(id.encode())
    conn.close()
    return redirect(url_for('index'))

@app.route('/edit/<id>', methods=['GET', 'POST'])
def edit_anime(id):
    conn = get_connection()
    table = conn.table(TABLE_NAME)

    if request.method == 'POST':
        data = request.form
        table.put(id.encode(), {
            b'info:title_english': data.get('title_english','').encode(),
            b'info:duration': data.get('duration','').encode(),
            b'info:score': data.get('score','').encode(),
            b'info:genres': data.get('genres','').encode(),
            b'info:studio': data.get('studio','').encode(),
            b'info:start_year': data.get('start_year','').encode(),

            b'detail:title_romaji': data.get('title_romaji','').encode(),
            b'detail:title_native': data.get('title_native','').encode(),
            b'detail:format': data.get('format','').encode(),
            b'detail:episodes': data.get('episodes','').encode(),
            b'detail:source': data.get('source','').encode(),
            b'detail:country': data.get('country','').encode(),
            b'detail:isAdult': data.get('isAdult','').encode(),

            b'social:meanScore': data.get('meanScore','').encode(),
            b'social:popularity': data.get('popularity','').encode(),
            b'social:favourites': data.get('favourites','').encode(),
            b'social:trending': data.get('trending','').encode(),
            b'social:tags': data.get('tags','').encode(),
            b'social:status': data.get('status','').encode(),
            b'social:season': data.get('season','').encode(),
            b'social:url': data.get('url','').encode(),
        })
        conn.close()
        return redirect(url_for('index'))

    # GET: Hiển thị dữ liệu hiện tại
    row = table.row(id.encode())
    item = {
        'id': id,
        'title_english': row.get(b'info:title_english', b'').decode(),
        'duration': row.get(b'info:duration', b'').decode(),
        'score': row.get(b'info:score', b'').decode(),
        'genres': row.get(b'info:genres', b'').decode(),
        'studio': row.get(b'info:studio', b'').decode(),
        'start_year': row.get(b'info:start_year', b'').decode(),

        'title_romaji': row.get(b'detail:title_romaji', b'').decode(),
        'title_native': row.get(b'detail:title_native', b'').decode(),
        'format': row.get(b'detail:format', b'').decode(),
        'episodes': row.get(b'detail:episodes', b'').decode(),
        'source': row.get(b'detail:source', b'').decode(),
        'country': row.get(b'detail:country', b'').decode(),
        'isAdult': row.get(b'detail:isAdult', b'').decode(),

        'meanScore': row.get(b'social:meanScore', b'').decode(),
        'popularity': row.get(b'social:popularity', b'').decode(),
        'favourites': row.get(b'social:favourites', b'').decode(),
        'trending': row.get(b'social:trending', b'').decode(),
        'tags': row.get(b'social:tags', b'').decode(),
        'status': row.get(b'social:status', b'').decode(),
        'season': row.get(b'social:season', b'').decode(),
        'url': row.get(b'social:url', b'').decode(),
    }

    conn.close()
    return render_template('edit.html', item=item)

@app.route('/backup', methods=['GET', 'POST'])
def backup():
    conn = get_connection()
    table_snap = conn.table(SNAPSHOT_TABLE)

    def run_hbase_shell(cmd):
        """Chạy 1 câu lệnh HBase shell qua stdin."""
        result = subprocess.run(
            ["hbase", "shell", "-n"],
            input=cmd + "\n",  # gửi vào stdin
            text=True,
            capture_output=True
        )
        return result

    # Khi bấm nút "Sao lưu"
    if request.method == 'POST':
        snapshot_name = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            # Disable bảng
            result_disable = run_hbase_shell(f"disable '{TABLE_NAME}'")
            if "ERROR" in result_disable.stdout or result_disable.returncode != 0:
                return jsonify({'error': f"Lỗi khi disable bảng: {result_disable.stdout}"}), 500

            # Tạo snapshot
            result_snapshot = run_hbase_shell(f"snapshot '{TABLE_NAME}', '{snapshot_name}'")
            if "ERROR" in result_snapshot.stdout or result_snapshot.returncode != 0:
                return jsonify({'error': f"Lỗi khi tạo snapshot: {result_snapshot.stdout}"}), 500

            # Enable lại bảng
            run_hbase_shell(f"enable '{TABLE_NAME}'")

            # Ghi log vào bảng snapshot trong HBase
            table_snap.put(snapshot_name.encode(), {
                b'info:name': snapshot_name.encode(),
                b'info:table': TABLE_NAME.encode(),
                b'info:created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S').encode(),
                b'info:status': b'success',
            })

            return jsonify({
                'message': f'Snapshot {snapshot_name} đã được tạo thành công và bảng đã được bật lại.'
            })

        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # Khi GET → hiển thị lịch sử sao lưu
    snapshots = []
    for key, data in table_snap.scan():
        snapshots.append({
            'id': key.decode(),
            'name': data.get(b'info:name', b'').decode(),
            'created_at': data.get(b'info:created_at', b'').decode(),
            'status': data.get(b'info:status', b'').decode(),
        })

    conn.close()
    return render_template('backup.html', snapshots=snapshots)

    # Hiển thị danh sách snapshot
    snapshots = []
    for key, data in table_snap.scan():
        snapshots.append({
            'name': key.decode(),
            'table': data.get(b'info:table', b'').decode(),
            'timestamp': data.get(b'info:timestamp', b'').decode(),
            'status': data.get(b'info:status', b'').decode()
        })

    conn.close()
    return render_template('backup.html', snapshots=sorted(snapshots, key=lambda x: x['timestamp'], reverse=True))

@app.route('/restore/<snapshot_name>', methods=['POST'])
def restore(snapshot_name):
    # Thực hiện phục hồi snapshot
    subprocess.run(
        ["hbase", "restore_snapshot", snapshot_name],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Cập nhật trạng thái
    conn = get_connection()
    table_snap = conn.table(SNAPSHOT_TABLE)
    table_snap.put(snapshot_name.encode(), {
        b'info:status': b'restored'
    })
    conn.close()
    return redirect(url_for('backup'))

@app.route('/charts')
def charts():
    p

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
