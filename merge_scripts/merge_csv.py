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

            # Выводим названия колонок для отладки
            print(f"   Колонки: {list(df.columns)}")

        except Exception as e:
            print(f"❌ Ошибка загрузки {file}: {e}")

    if not dataframes:
        print("❌ Не удалось загрузить ни одного файла")
        return

    # Объединяем все DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)

    before_dedup = len(combined_df)
    print(f"📊 Всего записей до обработки: {before_dedup}")
    print(f"📋 Доступные колонки: {list(combined_df.columns)}")

    # 🔥 ИСПРАВЛЕНИЕ: Используем правильные названия колонок из парсера
    def get_data_completeness_score(row):
        """Оцениваем полноту данных в строке"""
        score = 0

        # 🔥 ПРАВИЛЬНЫЕ НАЗВАНИЯ КОЛОНОК (из вашего парсера):
        # 'URL', 'название', 'Html', 'ОГРН', 'ИНН', 'Название юр лица',
        # 'Кол-во отзывов', 'рейтинг', 'Срок регистрации', 'Товары'

        # Даем больший вес важным полям
        if pd.notna(row.get('ОГРН')) and row.get('ОГРН') != '':
            score += 10
        if pd.notna(row.get('Название юр лица')) and row.get('Название юр лица') != '':
            score += 8
        if pd.notna(row.get('рейтинг')) and row.get('рейтинг') != '':
            score += 5
        if pd.notna(row.get('Кол-во отзывов')) and row.get('Кол-во отзывов') != '':
            score += 5
        if pd.notna(row.get('Срок регистрации')) and row.get('Срок регистрации') != '':
            score += 5
        if pd.notna(row.get('ИНН')) and row.get('ИНН') != '':
            score += 7

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

    # 🔥 ИСПРАВЛЕННАЯ ПРОВЕРКА КАЧЕСТВА ДАННЫХ
    try:
        # Проверяем наличие колонок перед анализом
        has_ogrn = 'ОГРН' in combined_df.columns
        has_legal_name = 'Название юр лица' in combined_df.columns

        if has_ogrn and has_legal_name:
            complete_records = len(combined_df[
                                       (combined_df['ОГРН'].notna()) &
                                       (combined_df['ОГРН'] != '') &
                                       (combined_df['Название юр лица'].notna()) &
                                       (combined_df['Название юр лица'] != '')
                                       ])
            print(f"   - Записей с полными юридическими данными: {complete_records}")
        else:
            print(f"   - Юридические данные: колонки не найдены")

    except Exception as e:
        print(f"   - Ошибка анализа качества данных: {e}")

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

        # Добавляем информацию о колонках
        f.write(f"Колонки в результате: {list(combined_df.columns)}\n\n")
        f.write("Обработанные файлы:\n")
        for file in csv_files:
            f.write(f"- {os.path.basename(file)}\n")

    print(f"📈 Статистика сохранена: {stats_file}")

    # Показываем пример результата
    print("\n🔍 Пример обработанных данных:")
    try:
        # Используем доступные колонки
        available_columns = ['URL', 'название', 'ОГРН', 'Название юр лица']
        display_columns = [col for col in available_columns if col in combined_df.columns]

        if display_columns:
            sample_data = combined_df[display_columns].head(3)
            for _, row in sample_data.iterrows():
                has_ogrn = pd.notna(row.get('ОГРН')) and row.get('ОГРН') != '' if 'ОГРН' in row else False
                status = "✅ ПОЛНЫЕ ДАННЫЕ" if has_ogrn else "❌ НЕПОЛНЫЕ ДАННЫЕ"
                name = row.get('название', 'нет названия') if 'название' in row else 'нет названия'
                print(f"   {status} | {row['URL']} | {name}")
        else:
            print("   Нет доступных колонок для отображения")

    except Exception as e:
        print(f"   Ошибка при выводе примера: {e}")


if __name__ == "__main__":
    merge_csv_files()