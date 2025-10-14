import pandas as pd
import glob
import os
import sys
from datetime import datetime


def merge_csv_files():
    """Объединение всех CSV файлов в один с сохранением наиболее полных данных"""
    data_dir = "/app/data"
    output_file = f"{data_dir}/combined_sellers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    print("🔍 Поиск CSV файлов...")

    # Ищем все CSV файлы
    csv_files = glob.glob(f"{data_dir}/sellers_*.csv")

    if not csv_files:
        print("❌ CSV файлы не найдены")
        return

    print(f"📁 Найдено файлов: {len(csv_files)}")

    # Читаем и объединяем все файлы
    dataframes = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, encoding='utf-8-sig')
            dataframes.append(df)
            print(f"✅ Загружен: {os.path.basename(file)} ({len(df)} строк)")
        except Exception as e:
            print(f"❌ Ошибка загрузки {file}: {e}")

    if dataframes:
        # Объединяем все DataFrame
        combined_df = pd.concat(dataframes, ignore_index=True)

        before_dedup = len(combined_df)
        print(f"📊 Всего записей до обработки: {before_dedup}")

        # 🔥 ИСПРАВЛЕНИЕ: Сохраняем наиболее полные данные для каждого URL
        def get_data_completeness_score(row):
            """Оцениваем полноту данных в строке"""
            score = 0
            # Даем больший вес важным полям
            if pd.notna(row.get('ОГРН')) and row.get('ОГРН') != '':
                score += 10
            if pd.notna(row.get('Название_юр_лица')) and row.get('Название_юр_лица') != '':
                score += 8
            if pd.notna(row.get('Рейтинг')) and row.get('Рейтинг') != '':
                score += 5
            if pd.notna(row.get('Отзывы')) and row.get('Отзывы') != '':
                score += 5
            if pd.notna(row.get('Заказы')) and row.get('Заказы') != '':
                score += 5
            if pd.notna(row.get('Срок_регистрации')) and row.get('Срок_регистрации') != '':
                score += 5
            # Учитываем количество заполненных полей
            filled_fields = sum(1 for value in row.values if pd.notna(value) and value != '')
            score += filled_fields
            return score

        # Добавляем столбец с оценкой полноты данных
        combined_df['_completeness_score'] = combined_df.apply(get_data_completeness_score, axis=1)

        # Сортируем по полноте данных (самые полные записи - первыми)
        combined_df = combined_df.sort_values('_completeness_score', ascending=False)

        # Удаляем дубликаты по URL, оставляя самые полные записи
        combined_df = combined_df.drop_duplicates(subset=['URL'], keep='first')

        # Удаляем временный столбец
        combined_df = combined_df.drop('_completeness_score', axis=1)

        after_dedup = len(combined_df)

        # Сохраняем объединенный файл
        combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')

        print(f"🎉 Объединенный файл сохранен: {output_file}")
        print(f"📊 Статистика:")
        print(f"   - Исходных файлов: {len(csv_files)}")
        print(f"   - Всего записей до удаления дубликатов: {before_dedup}")
        print(f"   - Всего записей после удаления дубликатов: {after_dedup}")
        print(f"   - Удалено дубликатов: {before_dedup - after_dedup}")

        # Анализ качества данных
        complete_records = len(combined_df[
                                   (combined_df['ОГРН'].notna()) &
                                   (combined_df['ОГРН'] != '') &
                                   (combined_df['Название_юр_лица'].notna()) &
                                   (combined_df['Название_юр_лица'] != '')
                                   ])
        print(f"   - Записей с полными юридическими данными: {complete_records}")

        # Создаем файл со статистикой
        stats_file = f"{data_dir}/merge_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(stats_file, 'w', encoding='utf-8') as f:
            f.write("Статистика объединения CSV\n")
            f.write("=" * 40 + "\n")
            f.write(f"Дата: {datetime.now()}\n")
            f.write(f"Исходных файлов: {len(csv_files)}\n")
            f.write(f"Всего записей до удаления дубликатов: {before_dedup}\n")
            f.write(f"Всего записей после удаления дубликатов: {after_dedup}\n")
            f.write(f"Удалено дубликатов: {before_dedup - after_dedup}\n")
            f.write(f"Записей с полными юр. данными: {complete_records}\n")
            f.write(f"Файл результата: {output_file}\n\n")
            f.write("Обработанные файлы:\n")
            for file in csv_files:
                f.write(f"- {os.path.basename(file)}\n")

        print(f"📈 Статистика сохранена: {stats_file}")

        # Показываем пример результата
        print("\n🔍 Пример обработанных данных:")
        sample_data = combined_df[['URL', 'Название', 'ОГРН', 'Название_юр_лица']].head(3)
        for _, row in sample_data.iterrows():
            status = "✅ ПОЛНЫЕ ДАННЫЕ" if pd.notna(row['ОГРН']) and row['ОГРН'] != '' else "❌ НЕПОЛНЫЕ ДАННЫЕ"
            print(f"   {status} | {row['URL']} | {row['Название']}")

    else:
        print("❌ Не удалось загрузить ни одного файла")


if __name__ == "__main__":
    merge_csv_files()