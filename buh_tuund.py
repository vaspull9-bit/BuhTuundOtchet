import sys
import os
import sqlite3
from datetime import datetime
from PyQt6.QtWidgets import *
from PyQt6.QtCore import *
from PyQt6.QtGui import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
import docx
from docx.shared import Inches, Pt
import openpyxl
from openpyxl.styles import Font, Alignment
from openpyxl.drawing.image import Image as ExcelImage
import io
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.fonts import addMapping
import re
from datetime import datetime

# ==================== БАЗА ДАННЫХ ====================
class DatabaseManager:
    def __init__(self, db_path='buh_tuund.db'):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period TEXT,
                company TEXT,
                product_group TEXT,
                nomenclature TEXT,
                revenue REAL,
                vat_in_revenue REAL,
                cost_price REAL,
                gross_profit REAL,
                sales_expenses REAL,
                other_income_expenses REAL,
                net_profit REAL,
                vat_deductible REAL,
                vat_to_budget REAL,
                quantity INTEGER,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS import_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                records_count INTEGER,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()

    def save_data(self, df):
        """
        Сохраняет данные из DataFrame в таблицу reports.
        Предварительно приводит колонки к нужным типам.
        Возвращает количество сохранённых записей.
        """
        # Создаём копию, чтобы не менять исходный df
        df_to_save = df.copy()

        # Список числовых колонок (REAL)
        numeric_cols = ['revenue', 'vat_in_revenue', 'cost_price', 'gross_profit',
                        'sales_expenses', 'other_income_expenses', 'net_profit',
                        'vat_deductible', 'vat_to_budget']
        # Приводим каждую к float, заменяем NaN на 0.0
        for col in numeric_cols:
            if col in df_to_save.columns:
                df_to_save[col] = pd.to_numeric(df_to_save[col], errors='coerce').fillna(0.0)

        # Колонка quantity должна быть целым числом
        if 'quantity' in df_to_save.columns:
            df_to_save['quantity'] = pd.to_numeric(df_to_save['quantity'], errors='coerce').fillna(0).astype(int)

        # Остальные колонки (period, company, product_group, nomenclature) уже строки, оставляем как есть

        # Вставляем в базу данных
        df_to_save.to_sql('reports', self.conn, if_exists='append', index=False)
        self.conn.commit()

        # Записываем в историю (можно добавить имя файла, но его нет в параметрах; можно передавать отдельно)
        # Пока пропустим или оставим как есть
        return len(df_to_save)

    def get_all_data(self):
        query = "SELECT * FROM reports ORDER BY period DESC, company"
        return pd.read_sql_query(query, self.conn)

    def get_filtered_data(self, company=None, period=None, product_group=None):
        query = "SELECT * FROM reports WHERE 1=1"
        params = []
        if company and company != "Все компании":
            query += " AND company = ?"
            params.append(company)
        if period and period != "Все периоды":
            query += " AND period = ?"
            params.append(period)
        if product_group and product_group != "Все группы":
            query += " AND product_group = ?"
            params.append(product_group)
        query += " ORDER BY period DESC, company"
        return pd.read_sql_query(query, self.conn, params=params)

# ==================== ГЛАВНОЕ ОКНО ====================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = DatabaseManager()
        self.current_df = None
        self.init_ui()
    
    def init_ui(self):
        self.setWindowTitle("BuhTuundOtchet v1.0")
        self.setGeometry(100, 100, 1400, 800)
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QToolBar {
                background-color: #2c3e50;
                spacing: 5px;
                padding: 5px;
            }
            QToolButton {
                background-color: #3498db;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 8px 12px;
                font-weight: bold;
            }
            QToolButton:hover {
                background-color: #2980b9;
            }
            QToolButton:pressed {
                background-color: #1c638e;
            }
            QTableView {
                background-color: white;
                alternate-background-color: #f8f9fa;
                selection-background-color: #3498db;
                gridline-color: #dee2e6;
                font-size: 11pt;
            }
            QHeaderView::section {
                background-color: #34495e;
                color: white;
                padding: 8px;
                border: none;
                font-weight: bold;
            }
            QComboBox, QLineEdit {
                padding: 6px;
                border: 1px solid #bdc3c7;
                border-radius: 4px;
                background-color: white;
            }
            QLabel {
                font-weight: bold;
                color: #2c3e50;
            }
        """)
        
        # Создание современного тулбара
        self.create_toolbar()
        
        # Центральный виджет с таблицей и графиками
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Панель фильтров
        filter_layout = QHBoxLayout()
        
        self.company_combo = QComboBox()
        self.company_combo.addItems(["Все компании", "ООО 'Ромашка'", "ООО 'Василек'"])
        
        self.period_combo = QComboBox()
        self.period_combo.addItems(["Все периоды", "01.2026", "12.2025", "11.2025"])
        
        self.group_combo = QComboBox()
        self.group_combo.addItems(["Все группы", "Электроника", "Мебель", "Офисная техника"])
        
        filter_layout.addWidget(QLabel("Компания:"))
        filter_layout.addWidget(self.company_combo)
        filter_layout.addWidget(QLabel("Период:"))
        filter_layout.addWidget(self.period_combo)
        filter_layout.addWidget(QLabel("Товарная группа:"))
        filter_layout.addWidget(self.group_combo)
        
        self.apply_filter_btn = QPushButton("Применить фильтр")
        self.apply_filter_btn.clicked.connect(self.apply_filters)
        self.apply_filter_btn.setStyleSheet("""
            QPushButton {
                background-color: #27ae60;
                color: white;
                font-weight: bold;
                padding: 8px 16px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #219653;
            }
        """)
        filter_layout.addWidget(self.apply_filter_btn)
        
        filter_layout.addStretch()
        main_layout.addLayout(filter_layout)
        
        # Создание вкладок
        self.tab_widget = QTabWidget()
        self.tab_widget.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #bdc3c7;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #ecf0f1;
                padding: 10px 20px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
            }
            QTabBar::tab:selected {
                background-color: #3498db;
                color: white;
                font-weight: bold;
            }
        """)
        
        # Вкладка с таблицей
        self.table_tab = QWidget()
        table_layout = QVBoxLayout(self.table_tab)
        
        # Таблица данных
        self.table_view = QTableView()
        self.table_model = QStandardItemModel()
        self.table_view.setModel(self.table_model)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(True)
        
        # Настройка заголовков таблицы
        headers = [
            "Период", "Компания", "Товарная группа", "Номенклатура",
            "Выручка (с НДС)", "НДС в выручке", "Себестоимость",
            "Валовая прибыль", "Расходы на продажу", "Прочие доходы/расходы",
            "Чистая прибыль", "НДС к вычету", "НДС К УПЛАТЕ", "Оборот (кол-во)"
        ]
        self.table_model.setHorizontalHeaderLabels(headers)
        
        table_layout.addWidget(self.table_view)
        
        # Панель итогов под таблицей
        summary_layout = QHBoxLayout()
        
        self.total_label = QLabel("Итого по фильтру:")
        self.total_label.setStyleSheet("font-size: 12pt; font-weight: bold; color: #2c3e50;")
        
        self.revenue_label = QLabel("Выручка: 0 ₽")
        self.vat_label = QLabel("НДС к уплате: 0 ₽")
        self.profit_label = QLabel("Чистая прибыль: 0 ₽")
        
        for label in [self.revenue_label, self.vat_label, self.profit_label]:
            label.setStyleSheet("""
                QLabel {
                    background-color: #ecf0f1;
                    padding: 8px 12px;
                    border-radius: 4px;
                    font-weight: bold;
                    color: #2c3e50;
                    border: 1px solid #bdc3c7;
                }
            """)
        
        summary_layout.addWidget(self.total_label)
        summary_layout.addWidget(self.revenue_label)
        summary_layout.addWidget(self.vat_label)
        summary_layout.addWidget(self.profit_label)
        summary_layout.addStretch()
        
        table_layout.addLayout(summary_layout)
        
        # Вкладка с графиками
        self.charts_tab = QWidget()
        charts_layout = QVBoxLayout(self.charts_tab)
        
        # Создание графиков
        self.figure, self.axes = plt.subplots(2, 2, figsize=(12, 10))
        self.figure.patch.set_facecolor('#f5f5f5')
        self.canvas = FigureCanvas(self.figure)
        
        charts_layout.addWidget(self.canvas)
        
        # Кнопки обновления графиков
        charts_btn_layout = QHBoxLayout()
        self.update_charts_btn = QPushButton("Обновить графики")
        self.update_charts_btn.clicked.connect(self.update_charts)
        self.update_charts_btn.setStyleSheet(self.apply_filter_btn.styleSheet())
        
        charts_btn_layout.addWidget(self.update_charts_btn)
        charts_btn_layout.addStretch()
        charts_layout.addLayout(charts_btn_layout)
        
        # Добавление вкладок
        self.tab_widget.addTab(self.table_tab, "📊 Таблица данных")
        self.tab_widget.addTab(self.charts_tab, "📈 Графики и анализ")
        
        main_layout.addWidget(self.tab_widget)
        
        # Загрузка начальных данных
        self.load_initial_data()

    def _get_header_text(self, file_path, rows=5):
        """
        Читает первые rows строк файла как текст.
        Все данные преобразуются в строки, NaN заменяются пустой строкой.
        Возвращает единую строку, содержащую все ячейки через пробел.
        """
        try:
            df_header = pd.read_excel(file_path, nrows=rows, header=None, dtype=str)
        except Exception as e:
            # Если не удалось прочитать с dtype=str, пробуем без dtype (для старых файлов)
            df_header = pd.read_excel(file_path, nrows=rows, header=None)
            df_header = df_header.astype(str)
        df_header = df_header.fillna('')
        return ' '.join(df_header.values.flatten())

    def download_template(self):
        """Создаёт и сохраняет шаблон Excel с нужными колонками"""
        template_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить шаблон", "шаблон_сводного_отчета.xlsx", "Excel Files (*.xlsx)"
        )
        if not template_path:
            return

        try:
            # Создаём пустой DataFrame с нужными колонками (русские названия)
            columns_ru = [
                'Период', 'Компания', 'Товарная группа', 'Номенклатура',
                'Выручка (с НДС)', 'НДС в выручке', 'Себестоимость',
                'Валовая прибыль', 'Расходы на продажу', 'Прочие доходы/расходы',
                'Чистая прибыль', 'НДС к вычету', 'НДС К УПЛАТЕ', 'Оборот (кол-во)'
            ]
            df_template = pd.DataFrame(columns=columns_ru)

            # Добавляем строку с примером (для удобства)
            example_row = {
                'Период': '01.2026',
                'Компания': 'ООО "Ромашка"',
                'Товарная группа': 'Электроника',
                'Номенклатура': 'Смартфон X',
                'Выручка (с НДС)': 1200000,
                'НДС в выручке': 200000,
                'Себестоимость': 800000,
                'Валовая прибыль': 400000,
                'Расходы на продажу': 50000,
                'Прочие доходы/расходы': 0,
                'Чистая прибыль': 350000,
                'НДС к вычету': 90000,
                'НДС К УПЛАТЕ': 110000,
                'Оборот (кол-во)': 100
            }
            df_template = pd.concat([df_template, pd.DataFrame([example_row])], ignore_index=True)

            # Сохраняем
            df_template.to_excel(template_path, index=False)
            QMessageBox.information(self, "Успех", f"Шаблон сохранён:\n{template_path}")

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось создать шаблон:\n{str(e)}")
    
    def create_toolbar(self):
        toolbar = QToolBar("Главная панель")
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(24, 24))
        self.addToolBar(toolbar)
        
         # Кнопка загрузки файлов (мультивыбор)
        load_files_action = QAction(QIcon.fromTheme("document-open"), "Загрузить файлы", self)
        load_files_action.triggered.connect(self.load_files)
        load_files_action.setShortcut("Ctrl+O")
        toolbar.addAction(load_files_action)

        # Кнопка загрузки папки (рекурсивно)
        load_folder_action = QAction(QIcon.fromTheme("folder-open"), "Загрузить папку", self)
        load_folder_action.triggered.connect(self.load_folder)
        toolbar.addAction(load_folder_action)
            
        toolbar.addSeparator()

         # Кнопка скачивания шаблона
        download_template_action = QAction("📥 Скачать шаблон", self)
        download_template_action.triggered.connect(self.download_template)
        toolbar.addAction(download_template_action)
        
        # Кнопка экспорта в Excel
        export_excel_action = QAction("📊 Экспорт в Excel", self)
        export_excel_action.triggered.connect(self.export_to_excel)
        toolbar.addAction(export_excel_action)
        
        # Кнопка экспорта в PDF
        export_pdf_action = QAction("📄 Экспорт в PDF", self)
        export_pdf_action.triggered.connect(self.export_to_pdf)
        toolbar.addAction(export_pdf_action)
        
        # Кнопка экспорта в Word
        export_word_action = QAction("📝 Экспорт в Word", self)
        export_word_action.triggered.connect(self.export_to_word)
        toolbar.addAction(export_word_action)
        
        toolbar.addSeparator()
        
        # Кнопка быстрого отчета
        report_action = QAction("📋 Быстрый отчет", self)
        report_action.triggered.connect(self.generate_quick_report)
        toolbar.addAction(report_action)
        
        toolbar.addSeparator()
        
        # Кнопка "О программе"
        about_action = QAction("ℹ️ О программе", self)
        about_action.triggered.connect(self.show_about)
        toolbar.addAction(about_action)
    
    def load_initial_data(self):
        """Загрузка начальных демо-данных"""
        demo_data = {
            'period': ['01.2026', '01.2026', '01.2026', '12.2025', '12.2025'],
            'company': ['ООО "Ромашка"', 'ООО "Ромашка"', 'ООО "Василек"', 'ООО "Ромашка"', 'ООО "Василек"'],
            'product_group': ['Электроника', 'Электроника', 'Мебель', 'Электроника', 'Офисная техника'],
            'nomenclature': ['Смартфон X', 'Ноутбук Y', 'Стул офисный', 'Планшет Z', 'Принтер ABC'],
            'revenue': [1200000, 1800000, 600000, 900000, 450000],
            'vat_in_revenue': [200000, 300000, 100000, 150000, 75000],
            'cost_price': [800000, 1200000, 350000, 600000, 300000],
            'gross_profit': [400000, 600000, 250000, 300000, 150000],
            'sales_expenses': [50000, 75000, 30000, 40000, 20000],
            'other_income_expenses': [0, 0, 10000, -5000, 0],
            'net_profit': [350000, 525000, 210000, 255000, 130000],
            'vat_deductible': [90000, 150000, 40000, 70000, 35000],
            'vat_to_budget': [110000, 150000, 60000, 80000, 40000],
            'quantity': [100, 60, 200, 75, 50]
        }
        
        self.current_df = pd.DataFrame(demo_data)
        self.display_data(self.current_df)
        self.update_totals()
        self.update_charts()

    def load_files(self):
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, "Выберите файлы Excel", "", "Excel Files (*.xlsx *.xls)"
        )
        if file_paths:
            self.process_files(file_paths)


    def load_folder(self):
        """Загрузка всех Excel-файлов из выбранной папки и её подпапок"""
        folder_path = QFileDialog.getExistingDirectory(self, "Выберите папку")
        if not folder_path:
            return

        excel_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith(('.xlsx', '.xls')):
                    excel_files.append(os.path.join(root, file))

        if not excel_files:
            QMessageBox.information(self, "Информация", "В выбранной папке нет Excel-файлов.")
            return

        self.process_files(excel_files)

    def process_files(self, file_paths):
        total = len(file_paths)
        if total == 0:
            return

        progress = QProgressDialog("Загрузка файлов...", "Отмена", 0, total, self)
        progress.setWindowModality(Qt.WindowModality.WindowModal)

        success_count = 0
        error_files = []

        for i, file_path in enumerate(file_paths):
            if progress.wasCanceled():
                break
            progress.setValue(i)
            progress.setLabelText(f"Обработка: {os.path.basename(file_path)}")

            try:
                saved = self._import_excel_file(file_path)
                if saved > 0:
                    success_count += 1
                # Если saved == 0, файл просто не подошёл – это не ошибка
            except Exception as e:
                error_files.append(f"{os.path.basename(file_path)}: {str(e)}")

        progress.setValue(total)

        # Обновляем отображение только если есть новые записи
        if success_count > 0:
            self.current_df = self.db.get_all_data()
            # Принудительное преобразование всех числовых колонок
            num_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses',
                'other_income_expenses','net_profit','vat_deductible','vat_to_budget','quantity']
            for col in num_cols:
                if col in self.current_df.columns:
                    self.current_df[col] = pd.to_numeric(self.current_df[col], errors='coerce').fillna(0)
            self.current_df['quantity'] = self.current_df['quantity'].astype(int) 
           
           
            print(f"Загружено записей из БД: {len(self.current_df)}")
            self.display_data(self.current_df)
            self.update_totals()
            # Защита от NaN в графиках
            try:
                self.update_charts()
            except Exception as e:
                print(f"Ошибка при обновлении графиков: {e}")

        msg = f"Успешно загружено: {success_count} из {total}"
        if error_files:
            msg += "\n\nОшибки:\n" + "\n".join(error_files[:5])
            if len(error_files) > 5:
                msg += f"\n... и ещё {len(error_files)-5} ошибок"
        QMessageBox.information(self, "Результат загрузки", msg)

    def _extract_company_from_text(self, text):
        """Извлекает название компании из текста (ищем ООО, ИП и т.п.)"""
        import re
        # Ищем ООО "Название", ИП "Название" и т.п.
        match = re.search(r'(ООО|ИП|ЗАО|ОАО)\s*[«"]?([^»"\s]+)[»"]?', text)
        if match:
            return match.group(0).replace('"', '').replace('«', '').replace('»', '').strip()
        # Если не нашли, пробуем взять первую строку после "Покупатель"
        lines = text.split('\n')
        for line in lines:
            if 'покупатель' in line.lower():
                parts = line.split()
                if len(parts) > 1:
                    return parts[1].strip('"«»')
        return "Неизвестно"

    def _extract_period_from_text(self, text, file_path):
        """Извлекает период (месяц.год) из текста или имени файла"""
        import re
        # Ищем "за 2025 г." или "с 01.01.2025 по 31.03.2025"
        match = re.search(r'за\s+(\d{4})\s*г', text)
        if match:
            year = match.group(1)
            # Пробуем найти месяц по названию
            month_match = re.search(r'(январ\w+|феврал\w+|март\w+|апрел\w+|май\w+|июн\w+|июл\w+|август\w+|сентябр\w+|октябр\w+|ноябр\w+|декабр\w+)', text.lower())
            months = {'январ': '01', 'феврал': '02', 'март': '03', 'апрел': '04', 'май': '05', 'июн': '06',
                    'июл': '07', 'август': '08', 'сентябр': '09', 'октябр': '10', 'ноябр': '11', 'декабр': '12'}
            if month_match:
                for ru, num in months.items():
                    if ru in month_match.group():
                        return f"{num}.{year}"
            # Ищем даты "с ... по ..."
            period_match = re.search(r'с\s+(\d{2})\.(\d{2})\.(\d{4})\s+по\s+(\d{2})\.(\d{2})\.(\d{4})', text)
            if period_match:
                start_day, start_month, start_year, end_day, end_month, end_year = period_match.groups()
                return f"{end_month}.{end_year}"
            return f"12.{year}"
        # Если в тексте нет, ищем в имени файла
        base = os.path.basename(file_path)
        match = re.search(r'(\d{4})', base)
        if match:
            return f"12.{match.group(1)}"
        return "01.2026"

    def _flatten_text(self, df, rows):
        """Преобразует указанные строки DataFrame в одну строку текста"""
        if isinstance(rows, slice):
            subset = df.iloc[rows]
        else:
            subset = df.iloc[list(rows)]
        # Заменяем NaN на пустую строку
        subset = subset.fillna('')
        # Преобразуем каждое значение в строку
        strings = []
        for _, row in subset.iterrows():
            for cell in row:
                strings.append(str(cell))
        return ' '.join(strings)

    def _clean_number(self, value):
        """Преобразует любой вход в число с плавающей точкой (float)"""
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, bytes):
            # Пытаемся декодировать байты в строку
            try:
                s = value.decode('utf-8')
            except:
                s = str(value)
        else:
            s = str(value)
        # Очистка строки: убираем пробелы, заменяем запятую на точку
        s = s.strip().replace(' ', '').replace(',', '.').replace('−', '-').replace('—', '-')
        import re
        s = re.sub(r'[^\d.-]', '', s)
        try:
            return float(s) if s else 0.0
        except:
            return 0.0

    def _month_name_to_number(self, month_name):
        """Преобразует русское название месяца в номер"""
        month_names = {
            'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04', 'май': '05', 'июн': '06',
            'июл': '07', 'авг': '08', 'сен': '09', 'окт': '10', 'ноя': '11', 'дек': '12'
        }
        for key, num in month_names.items():
            if key in month_name.lower():
                return num
        return '01'    

    def _import_excel_file(self, file_path):
        """Универсальный импорт: определяет тип файла по первым строкам и вызывает нужный парсер."""
        if file_path.lower().endswith('.xls'):
            try:
                import xlrd
            except ImportError:
                raise ImportError("Для чтения файлов .xls установите xlrd: pip install xlrd")

        # Читаем первые 10 строк для анализа (больше, чтобы точно определить тип)
        try:
            df_preview = pd.read_excel(file_path, nrows=10, header=None, dtype=str)
        except:
            df_preview = pd.read_excel(file_path, nrows=10, header=None)
            df_preview = df_preview.astype(str)
        df_preview = df_preview.fillna('')
        preview_text = ' '.join(df_preview.values.flatten()).lower()

        # Определяем тип (ищем характерные фразы)
        if 'книга покупок' in preview_text:
            return self._parse_purchase_ledger_detailed(file_path)
        elif 'книга продаж' in preview_text:
            return self._parse_sales_ledger_detailed(file_path)
        elif 'оборотно-сальдовая ведомость по счету 19' in preview_text or 'счет 19' in preview_text:
            return self._parse_osv_19_detailed(file_path)
        elif 'оборотно-сальдовая ведомость по счету 41' in preview_text or 'счет 41' in preview_text:
            return self._parse_osv_41_detailed(file_path)
        elif 'оборотно-сальдовая ведомость по счету 44' in preview_text or 'счет 44' in preview_text:
            return self._parse_osv_44_detailed(file_path)
        elif 'оборотно-сальдовая ведомость по счету 90' in preview_text or 'счет 90' in preview_text:
            return self._parse_osv_90_detailed(file_path)
        elif 'оборотно-сальдовая ведомость по счету 91' in preview_text or 'счет 91' in preview_text:
            return self._parse_osv_91_detailed(file_path)
        elif 'отчет по продажам за' in preview_text:
            return self._parse_sales_report_detailed(file_path)
        else:
            # Если не распознали – пробуем как сводный шаблон
            return self._import_legacy(file_path)

    # ========== КНИГА ПОКУПОК (построчно) ==========
    def _parse_purchase_ledger_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        print(f"\n--- Книга покупок: {os.path.basename(file_path)} ---")
        for i in range(min(15, len(df))):
            print(f"Строка {i}: {df.iloc[i].tolist()}")

        # Находим строку с номерами колонок (1, 2, 3, ...)
        start_row = None
        for i in range(len(df)):
            row = df.iloc[i]
            if len(row) > 1:
                first = str(row[0]).strip()
                second = str(row[1]).strip()
                if first == '1' and second == '2':
                    start_row = i + 1
                    break
        if start_row is None:
            raise ValueError("Не удалось найти строку с номерами колонок в книге покупок")

        data_rows = []
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            if pd.isna(row[0]) or str(row[0]).strip() == '':
                continue
            if 'всего' in str(row[0]).lower():
                total_vat = self._clean_number(row[14]) if len(row) > 14 else 0.0  # колонка 15
                if total_vat == 0.0 and len(row) > 59:
                    total_vat = self._clean_number(row[59])
                if total_vat != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'НДС к вычету',
                        'nomenclature': 'Итого по книге покупок',
                        'revenue': 0.0,
                        'vat_in_revenue': 0.0,
                        'cost_price': 0.0,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': 0.0,
                        'net_profit': 0.0,
                        'vat_deductible': total_vat,
                        'vat_to_budget': 0.0,
                        'quantity': 0
                    })
                break
            seller = str(row[8]).strip() if len(row) > 8 and not pd.isna(row[8]) else ''
            if not seller:
                continue
            cost = self._clean_number(row[13]) if len(row) > 13 else 0.0
            vat = self._clean_number(row[14]) if len(row) > 14 else 0.0
            if vat == 0.0 and len(row) > 18:
                vat = self._clean_number(row[18])
            if cost == 0.0 and vat == 0.0:
                continue
            data_rows.append({
                'period': period,
                'company': company,
                'product_group': 'Покупки',
                'nomenclature': seller.strip(),
                'revenue': cost,
                'vat_in_revenue': 0.0,
                'cost_price': 0.0,
                'gross_profit': 0.0,
                'sales_expenses': 0.0,
                'other_income_expenses': 0.0,
                'net_profit': 0.0,
                'vat_deductible': vat,
                'vat_to_budget': 0.0,
                'quantity': 0
            })
        if not data_rows:
            print("Нет данных в книге покупок")
            return 0
        df_result = pd.DataFrame(data_rows)
        df_result['quantity'] = df_result['quantity'].astype(int)
        numeric_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses','other_income_expenses','net_profit','vat_deductible','vat_to_budget']
        for col in numeric_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из книги покупок")
        return saved

    # ========== КНИГА ПРОДАЖ (построчно) ==========
    def _parse_sales_ledger_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        print(f"\n--- Книга продаж: {os.path.basename(file_path)} ---")
        for i in range(min(15, len(df))):
            print(f"Строка {i}: {df.iloc[i].tolist()}")

        # Находим строку с номерами колонок (обычно после сложных заголовков)
        start_row = None
        for i in range(len(df)):
            row = df.iloc[i]
            if len(row) > 1:
                first = str(row[0]).strip()
                second = str(row[1]).strip()
                # Проверяем, что это похоже на номера колонок (1, 2, 3...)
                if first.isdigit() and second.isdigit() and int(first) == 1 and int(second) == 2:
                    start_row = i + 1
                    break
        if start_row is None:
            raise ValueError("Не удалось найти строку с номерами колонок в книге продаж")

        data_rows = []
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            if pd.isna(row[0]) or str(row[0]).strip() == '':
                continue
            if 'всего' in str(row[0]).lower():
                # Можно извлечь итоговый НДС, если нужно
                total_vat = self._clean_number(row[14]) if len(row) > 14 else 0.0
                if total_vat != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'НДС начисленный',
                        'nomenclature': 'Итого по книге продаж',
                        'revenue': 0.0,
                        'vat_in_revenue': total_vat,
                        'cost_price': 0.0,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': 0.0,
                        'net_profit': 0.0,
                        'vat_deductible': 0.0,
                        'vat_to_budget': total_vat,
                        'quantity': 0
                    })
                break

            buyer = str(row[8]).strip() if len(row) > 8 and not pd.isna(row[8]) else ''
            if not buyer:
                continue
            revenue = self._clean_number(row[13]) if len(row) > 13 else 0.0  # стоимость с НДС
            vat = self._clean_number(row[14]) if len(row) > 14 else 0.0     # сумма НДС
            if revenue == 0.0 and vat == 0.0:
                continue
            data_rows.append({
                'period': period,
                'company': company,
                'product_group': 'Продажи',
                'nomenclature': buyer.strip(),
                'revenue': revenue,
                'vat_in_revenue': vat,
                'cost_price': 0.0,
                'gross_profit': 0.0,
                'sales_expenses': 0.0,
                'other_income_expenses': 0.0,
                'net_profit': 0.0,
                'vat_deductible': 0.0,
                'vat_to_budget': vat,
                'quantity': 0
            })
        if not data_rows:
            print("Нет данных в книге продаж")
            return 0
        df_result = pd.DataFrame(data_rows)
        df_result['quantity'] = df_result['quantity'].astype(int)
        numeric_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses','other_income_expenses','net_profit','vat_deductible','vat_to_budget']
        for col in numeric_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из книги продаж")
        return saved

    # ========== ОСВ 19 (по контрагентам) ==========
    def _parse_osv_19_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        print(f"\n--- ОСВ 19: {os.path.basename(file_path)} ---")
        for i in range(min(20, len(df))):
            print(f"Строка {i}: {df.iloc[i].tolist()}")

        # Поиск строки с 'Счет' и следующей строки с 'Контрагенты'
        header_row = None
        for i in range(len(df)-1):
            row = df.iloc[i]
            row_str = ' '.join([str(c).lower() for c in row if pd.notna(c)])
            if 'счет' in row_str:
                next_row = df.iloc[i+1]
                next_str = ' '.join([str(c).lower() for c in next_row if pd.notna(c)])
                if 'контрагенты' in next_str:
                    header_row = i
                    break
        if header_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 19")

        # Данные начинаются через 3 строки после header_row (после двух строк заголовков и строки 'Период')
        start_row = header_row + 3
        data_rows = []
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            if pd.isna(row[0]) or str(row[0]).strip() == '':
                continue
            if 'итого' in str(row[0]).lower():
                break
            kontragent = str(row[1]).strip() if len(row) > 1 and not pd.isna(row[1]) else ''
            if not kontragent:
                continue
            # Оборот дебет (НДС к вычету) – колонка 5 (индекс 5)
            vat = self._clean_number(row[5]) if len(row) > 5 else 0.0
            if vat == 0.0:
                continue
            data_rows.append({
                'period': period,
                'company': company,
                'product_group': 'НДС к вычету',
                'nomenclature': f"Контрагент: {kontragent}",
                'revenue': 0.0,
                'vat_in_revenue': 0.0,
                'cost_price': 0.0,
                'gross_profit': 0.0,
                'sales_expenses': 0.0,
                'other_income_expenses': 0.0,
                'net_profit': 0.0,
                'vat_deductible': vat,
                'vat_to_budget': 0.0,
                'quantity': 0
            })
        if not data_rows:
            print("Нет данных в ОСВ 19")
            return 0
        df_result = pd.DataFrame(data_rows)
        df_result['quantity'] = df_result['quantity'].astype(int)
        numeric_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses','other_income_expenses','net_profit','vat_deductible','vat_to_budget']
        for col in numeric_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из ОСВ 19")
        return saved

    # ========== ОСВ 41 (по номенклатуре) ==========
    def _parse_osv_41_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        print(f"\n--- ОСВ 41: {os.path.basename(file_path)} ---")
        for i in range(min(20, len(df))):
            print(f"Строка {i}: {df.iloc[i].tolist()}")

        # Поиск строки с 'Счет' и следующей строки с 'Номенклатура'
        header_row = None
        for i in range(len(df)-1):
            row = df.iloc[i]
            row_str = ' '.join([str(c).lower() for c in row if pd.notna(c)])
            if 'счет' in row_str:
                next_row = df.iloc[i+1]
                next_str = ' '.join([str(c).lower() for c in next_row if pd.notna(c)])
                if 'номенклатура' in next_str:
                    header_row = i
                    break
        if header_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 41")

        # Данные начинаются через 2 строки после header_row (после двух строк заголовков)
        start_row = header_row + 2
        data_rows = []
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            if len(row) < 2 or pd.isna(row[1]) or str(row[1]).strip() == '':
                continue
            nomenclature = str(row[1]).strip()
            if 'итого' in nomenclature.lower():
                continue
            # Оборот кредит (себестоимость) – колонка 6 (индекс 6)
            cost = self._clean_number(row[6]) if len(row) > 6 else 0.0
            if cost == 0.0:
                continue
            data_rows.append({
                'period': period,
                'company': company,
                'product_group': 'Товары',
                'nomenclature': nomenclature,
                'revenue': 0.0,
                'vat_in_revenue': 0.0,
                'cost_price': cost,
                'gross_profit': 0.0,
                'sales_expenses': 0.0,
                'other_income_expenses': 0.0,
                'net_profit': 0.0,
                'vat_deductible': 0.0,
                'vat_to_budget': 0.0,
                'quantity': 0
            })
        if not data_rows:
            print("Нет данных в ОСВ 41")
            return 0
        df_result = pd.DataFrame(data_rows)
        df_result['quantity'] = df_result['quantity'].astype(int)
        numeric_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses','other_income_expenses','net_profit','vat_deductible','vat_to_budget']
        for col in numeric_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из ОСВ 41")
        return saved

    # ========== ОСВ 44 (по статьям затрат) ==========
    def _parse_osv_44_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        print(f"\n--- ОСВ 44: {os.path.basename(file_path)} ---")
        for i in range(min(20, len(df))):
            print(f"Строка {i}: {df.iloc[i].tolist()}")

        # Поиск строки с 'Счет' и следующей строки с 'Статьи затрат'
        header_row = None
        for i in range(len(df)-1):
            row = df.iloc[i]
            row_str = ' '.join([str(c).lower() for c in row if pd.notna(c)])
            if 'счет' in row_str:
                next_row = df.iloc[i+1]
                next_str = ' '.join([str(c).lower() for c in next_row if pd.notna(c)])
                if 'статьи затрат' in next_str:
                    header_row = i
                    break
        if header_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 44")

        # Данные начинаются через 3 строки после header_row (после двух строк заголовков и строки 'Период')
        start_row = header_row + 3
        data_rows = []
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            if len(row) < 2 or pd.isna(row[1]) or str(row[1]).strip() == '':
                continue
            article = str(row[1]).strip()
            if 'итого' in article.lower():
                break
            # Оборот дебет (расходы) – колонка 3 (индекс 3)
            expenses = self._clean_number(row[3]) if len(row) > 3 else 0.0
            if expenses == 0.0:
                continue
            data_rows.append({
                'period': period,
                'company': company,
                'product_group': 'Расходы на продажу',
                'nomenclature': article,
                'revenue': 0.0,
                'vat_in_revenue': 0.0,
                'cost_price': 0.0,
                'gross_profit': 0.0,
                'sales_expenses': expenses,
                'other_income_expenses': 0.0,
                'net_profit': 0.0,
                'vat_deductible': 0.0,
                'vat_to_budget': 0.0,
                'quantity': 0
            })
        if not data_rows:
            print("Нет данных в ОСВ 44")
            return 0
        df_result = pd.DataFrame(data_rows)
        df_result['quantity'] = df_result['quantity'].astype(int)
        numeric_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses','other_income_expenses','net_profit','vat_deductible','vat_to_budget']
        for col in numeric_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из ОСВ 44")
        return saved

    # ========== ОСВ 90 (по субсчетам) ==========
    def _parse_osv_90_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        print(f"\n--- ОСВ 90: {os.path.basename(file_path)} ---")
        for i in range(min(20, len(df))):
            print(f"Строка {i}: {df.iloc[i].tolist()}")

        # Поиск строки с 'Счет' и 'Показатели' (обычно в одной строке)
        header_row = None
        for i in range(len(df)):
            row = df.iloc[i]
            row_str = ' '.join([str(c).lower() for c in row if pd.notna(c)])
            if 'счет' in row_str and 'показатели' in row_str:
                header_row = i
                break
        if header_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 90")

        # Данные начинаются со следующей строки после заголовков
        start_row = header_row + 1
        data_rows = []
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            if pd.isna(row[0]) or str(row[0]).strip() == '':
                continue
            account = str(row[0]).strip()
            if 'итого' in account.lower():
                break
            if '90.01' in account:
                revenue = self._clean_number(row[6]) if len(row) > 6 else 0.0  # кредит
                if revenue != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'Выручка',
                        'nomenclature': account,
                        'revenue': revenue,
                        'vat_in_revenue': 0.0,
                        'cost_price': 0.0,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': 0.0,
                        'net_profit': 0.0,
                        'vat_deductible': 0.0,
                        'vat_to_budget': 0.0,
                        'quantity': 0
                    })
            elif '90.02' in account:
                cost = self._clean_number(row[5]) if len(row) > 5 else 0.0  # дебет
                if cost != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'Себестоимость',
                        'nomenclature': account,
                        'revenue': 0.0,
                        'vat_in_revenue': 0.0,
                        'cost_price': cost,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': 0.0,
                        'net_profit': 0.0,
                        'vat_deductible': 0.0,
                        'vat_to_budget': 0.0,
                        'quantity': 0
                    })
            elif '90.03' in account:
                vat = self._clean_number(row[5]) if len(row) > 5 else 0.0  # дебет
                if vat != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'НДС начисленный',
                        'nomenclature': account,
                        'revenue': 0.0,
                        'vat_in_revenue': vat,
                        'cost_price': 0.0,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': 0.0,
                        'net_profit': 0.0,
                        'vat_deductible': 0.0,
                        'vat_to_budget': vat,
                        'quantity': 0
                    })
        if not data_rows:
            print("Субсчета 90.01-90.03 не найдены. Попытка извлечь итоги по счёту 90...")
            # Поиск строки с '90' (общий итог)
            for idx in range(start_row, len(df)):
                row = df.iloc[idx]
                if pd.isna(row[0]) or str(row[0]).strip() != '90':
                    continue
                # В этой строке могут быть общие обороты, но без разбивки на субсчета
                # Поэтому данные не сохраняем
                break
            if not data_rows:
                return 0
        df_result = pd.DataFrame(data_rows)
        df_result['quantity'] = df_result['quantity'].astype(int)
        numeric_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses','other_income_expenses','net_profit','vat_deductible','vat_to_budget']
        for col in numeric_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из ОСВ 90")
        return saved

    # ========== ОСВ 91 (по субсчетам) ==========
    def _parse_osv_91_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        print(f"\n--- ОСВ 91: {os.path.basename(file_path)} ---")
        for i in range(min(20, len(df))):
            print(f"Строка {i}: {df.iloc[i].tolist()}")

        # Поиск строки с 'Счет' и следующей строки с 'Прочие доходы и расходы'
        header_row = None
        for i in range(len(df)-1):
            row = df.iloc[i]
            row_str = ' '.join([str(c).lower() for c in row if pd.notna(c)])
            if 'счет' in row_str:
                next_row = df.iloc[i+1]
                next_str = ' '.join([str(c).lower() for c in next_row if pd.notna(c)])
                if 'прочие доходы и расходы' in next_str:
                    header_row = i
                    break
        if header_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 91")

        # Данные начинаются через 3 строки после header_row (после двух строк заголовков и строки 'Период')
        start_row = header_row + 3
        data_rows = []
        for idx in range(start_row, len(df)):
            row = df.iloc[idx]
            if pd.isna(row[0]) or str(row[0]).strip() == '':
                continue
            account = str(row[0]).strip()
            if 'итого' in account.lower():
                break
            if '91.01' in account:
                income = self._clean_number(row[5]) if len(row) > 5 else 0.0  # кредит
                if income != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'Прочие доходы',
                        'nomenclature': account,
                        'revenue': income,
                        'vat_in_revenue': 0.0,
                        'cost_price': 0.0,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': income,
                        'net_profit': 0.0,
                        'vat_deductible': 0.0,
                        'vat_to_budget': 0.0,
                        'quantity': 0
                    })
            elif '91.02' in account:
                expense = self._clean_number(row[4]) if len(row) > 4 else 0.0  # дебет
                if expense != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'Прочие расходы',
                        'nomenclature': account,
                        'revenue': 0.0,
                        'vat_in_revenue': 0.0,
                        'cost_price': 0.0,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': -expense,
                        'net_profit': 0.0,
                        'vat_deductible': 0.0,
                        'vat_to_budget': 0.0,
                        'quantity': 0
                    })
        if not data_rows:
            print("Субсчета 91.01-91.02 не найдены. Попытка извлечь итоги по счёту 91...")
            # Поиск строки с '91'
            for idx in range(start_row, len(df)):
                row = df.iloc[idx]
                if pd.isna(row[0]) or str(row[0]).strip() != '91':
                    continue
                credit = self._clean_number(row[5]) if len(row) > 5 else 0.0
                debit = self._clean_number(row[4]) if len(row) > 4 else 0.0
                if credit != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'Прочие доходы',
                        'nomenclature': '91 (кредит)',
                        'revenue': credit,
                        'vat_in_revenue': 0.0,
                        'cost_price': 0.0,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': credit,
                        'net_profit': 0.0,
                        'vat_deductible': 0.0,
                        'vat_to_budget': 0.0,
                        'quantity': 0
                    })
                if debit != 0.0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'Прочие расходы',
                        'nomenclature': '91 (дебет)',
                        'revenue': 0.0,
                        'vat_in_revenue': 0.0,
                        'cost_price': 0.0,
                        'gross_profit': 0.0,
                        'sales_expenses': 0.0,
                        'other_income_expenses': -debit,
                        'net_profit': 0.0,
                        'vat_deductible': 0.0,
                        'vat_to_budget': 0.0,
                        'quantity': 0
                    })
                break
            if not data_rows:
                return 0
        df_result = pd.DataFrame(data_rows)
        df_result['quantity'] = df_result['quantity'].astype(int)
        numeric_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses','other_income_expenses','net_profit','vat_deductible','vat_to_budget']
        for col in numeric_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из ОСВ 91")
        return saved

    # ========== ОТЧЁТ ПО ПРОДАЖАМ (по товарам и месяцам) ==========
    def _parse_sales_report_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period_base = self._extract_period_from_text(header_text, file_path)
        year = period_base.split('.')[1] if '.' in period_base else period_base

        print(f"\n--- Отчёт по продажам: {os.path.basename(file_path)} ---")
        for i in range(min(15, len(df))):
            print(f"Строка {i}: {df.iloc[i].tolist()}")

        # Ищем строку с "Номенклатура"
        start_row = None
        for i in range(len(df)):
            if 'Номенклатура' in str(df.iloc[i, 0]):
                start_row = i
                break
        if start_row is None:
            raise ValueError("Не удалось найти заголовок 'Номенклатура'")

        # Определяем месяцы
        months = []
        for r in [start_row, start_row+1]:
            if r >= len(df):
                continue
            row = df.iloc[r]
            for col_idx, val in enumerate(row):
                if isinstance(val, str) and any(m in val.lower() for m in ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']):
                    months.append((col_idx, val.strip()))
            if months:
                start_row = r
                break
        if not months:
            raise ValueError("Не удалось определить месяцы")

        data_rows = []
        data_start = start_row + 2
        for idx in range(data_start, len(df)):
            row = df.iloc[idx]
            if pd.isna(row[0]) or str(row[0]).strip() == '':
                continue
            nomenclature = str(row[0]).strip()
            if 'итого' in nomenclature.lower():
                continue
            for col_idx, month_name in months:
                month_num = self._month_name_to_number(month_name)
                period = f"{month_num}.{year}"
                qty = self._clean_number(row[col_idx]) if len(row) > col_idx else 0
                amount = self._clean_number(row[col_idx+1]) if len(row) > col_idx+1 else 0
                if qty == 0 and amount == 0:
                    continue
                vat = amount * 20 / 120 if amount != 0 else 0
                data_rows.append({
                    'period': period,
                    'company': company,
                    'product_group': 'Товары',
                    'nomenclature': nomenclature,
                    'revenue': amount,
                    'vat_in_revenue': vat,
                    'cost_price': 0,
                    'gross_profit': 0,
                    'sales_expenses': 0,
                    'other_income_expenses': 0,
                    'net_profit': 0,
                    'vat_deductible': 0,
                    'vat_to_budget': vat,
                    'quantity': int(qty) if qty else 0
                })
        if not data_rows:
            print("Нет данных в отчёте по продажам")
            return 0
        df_result = pd.DataFrame(data_rows)
        df_result['quantity'] = df_result['quantity'].astype(int)
        numeric_cols = ['revenue','vat_in_revenue','cost_price','gross_profit','sales_expenses','other_income_expenses','net_profit','vat_deductible','vat_to_budget']
        for col in numeric_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из отчёта по продажам")
        return saved

    # ---------- Парсер для книги покупок ----------
    def _parse_purchase_ledger(self, file_path):
        df = pd.read_excel(file_path, header=None)

        # --- Извлечение компании ---
        company = "Неизвестно"
        for i in range(min(5, len(df))):
            row = df.iloc[i]
            for j, cell in enumerate(row):
                if isinstance(cell, str) and 'покупатель' in cell.lower():
                    # Ищем следующую непустую ячейку
                    for k in range(j+1, len(row)):
                        val = row[k]
                        if pd.notna(val) and str(val).strip():
                            company = str(val).strip()
                            # Очищаем от кавычек и лишних пробелов
                            company = company.replace('"', '').replace('«', '').replace('»', '').strip()
                            break
                    break
            if company != "Неизвестно":
                break

        # Если не нашли, пробуем из имени файла
        if company == "Неизвестно":
            import re
            match = re.search(r'(ООО|ИП|ЗАО|ОАО)\s*[«"]?([^»"\s]+)', os.path.basename(file_path))
            if match:
                company = match.group(0)
            else:
                company = os.path.basename(file_path).split()[0]

        # --- Извлечение периода ---
        period = "03.2025"
        for i in range(min(5, len(df))):
            row = df.iloc[i]
            for cell in row:
                if isinstance(cell, str) and 'период с' in cell.lower():
                    import re
                    match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', cell)
                    if match:
                        day, month, year = match.groups()
                        period = f"{month}.{year}"
                        break
            if period != "03.2025":
                break

        # --- Поиск итоговой строки ---
        total_row = None
        for i in range(len(df)-1, max(0, len(df)-50), -1):
            cell_val = df.iloc[i, 0] if df.shape[1] > 0 else ''
            if pd.isna(cell_val):
                continue
            if 'всего' in str(cell_val).lower():
                total_row = df.iloc[i]
                break

        if total_row is None:
            print(f"Файл {os.path.basename(file_path)}: не найдена итоговая строка, пропускаем.")
            return 0

        # --- Извлечение суммы НДС (колонка 59) ---
        vat_sum = 0.0
        if len(total_row) > 59:
            vat_sum = self._clean_number(total_row[59])
        else:
            for col in [14, 18, 58, 60]:
                if len(total_row) > col:
                    vat_sum = self._clean_number(total_row[col])
                    if vat_sum != 0.0:
                        break

        if vat_sum == 0.0 or pd.isna(vat_sum):
            print(f"Файл {os.path.basename(file_path)}: сумма НДС не найдена, пропускаем.")
            return 0

        # --- Формирование записи с явными типами ---
        data_row = {
            'period': str(period),
            'company': str(company),
            'product_group': 'НДС к вычету',
            'nomenclature': 'Книга покупок',
            'revenue': 0.0,
            'vat_in_revenue': 0.0,
            'cost_price': 0.0,
            'gross_profit': 0.0,
            'sales_expenses': 0.0,
            'other_income_expenses': 0.0,
            'net_profit': 0.0,
            'vat_deductible': float(vat_sum),
            'vat_to_budget': 0.0,
            'quantity': 0
        }

        # Создаём DataFrame с правильными типами
        df_result = pd.DataFrame([data_row])
        df_result['quantity'] = df_result['quantity'].astype(int)
        df_result['vat_deductible'] = df_result['vat_deductible'].astype(float)
        # Остальные числовые колонки уже float

        # Сохраняем
        saved = self.db.save_data(df_result)
        print(f"Сохранено {saved} записей из файла {os.path.basename(file_path)}")
        return saved

    # ---------- Парсер для книги продаж ----------
    def _parse_sales_ledger(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._get_header_text(file_path, 5)
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        total_row = None
        for i in range(len(df)-1, max(0, len(df)-50), -1):
            cell_val = df.iloc[i, 0] if df.shape[1] > 0 else ''
            if pd.isna(cell_val):
                continue
            if 'всего' in str(cell_val).lower() or 'итого' in str(cell_val).lower():
                total_row = df.iloc[i]
                break

        if total_row is None:
            raise ValueError("Не удалось найти итоговую строку в книге продаж")

        vat_sum = self._clean_number(total_row[19]) if len(total_row) > 19 else 0
        if vat_sum == 0 and len(total_row) > 15:
            vat_sum = self._clean_number(total_row[15])

        data_row = {
            'period': period,
            'company': company,
            'product_group': 'НДС начисленный',
            'nomenclature': 'Книга продаж',
            'revenue': 0,
            'vat_in_revenue': vat_sum,
            'cost_price': 0,
            'gross_profit': 0,
            'sales_expenses': 0,
            'other_income_expenses': 0,
            'net_profit': 0,
            'vat_deductible': 0,
            'vat_to_budget': vat_sum,
            'quantity': 0
        }
        df_result = pd.DataFrame([data_row])
        return self.db.save_data(df_result)

    def _parse_osv_41(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._get_header_text(file_path, 5)
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        start_row = None
        for i in range(len(df)):
            cell_val = df.iloc[i, 0] if df.shape[1] > 0 else ''
            if pd.isna(cell_val):
                continue
            if 'счет' in str(cell_val).lower():
                cell_val2 = df.iloc[i, 1] if df.shape[1] > 1 else ''
                if not pd.isna(cell_val2) and 'номенклатура' in str(cell_val2).lower():
                    start_row = i
                    break

        if start_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 41")

        data_rows = []
        data_start = start_row + 1
        for idx in range(data_start, len(df)):
            row = df.iloc[idx]
            if len(row) < 2 or pd.isna(row[1]) or str(row[1]).strip() == '':
                continue
            nomenclature = str(row[1]).strip()
            if 'итого' in nomenclature.lower() or 'всего' in nomenclature.lower():
                continue
            cost = self._clean_number(row[7]) if len(row) > 7 else 0
            if cost == 0:
                continue
            data_rows.append({
                'period': period,
                'company': company,
                'product_group': 'Товары',
                'nomenclature': nomenclature,
                'revenue': 0,
                'vat_in_revenue': 0,
                'cost_price': cost,
                'gross_profit': 0,
                'sales_expenses': 0,
                'other_income_expenses': 0,
                'net_profit': 0,
                'vat_deductible': 0,
                'vat_to_budget': 0,
                'quantity': 0
            })
        if not data_rows:
            raise ValueError("Не удалось извлечь данные из ОСВ 41")
        df_result = pd.DataFrame(data_rows)
        return self.db.save_data(df_result)

    def _parse_osv_44(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._get_header_text(file_path, 5)
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        start_row = None
        for i in range(len(df)):
            cell_val = df.iloc[i, 0] if df.shape[1] > 0 else ''
            if pd.isna(cell_val):
                continue
            if 'счет' in str(cell_val).lower():
                cell_val2 = df.iloc[i, 1] if df.shape[1] > 1 else ''
                if not pd.isna(cell_val2) and 'статьи затрат' in str(cell_val2).lower():
                    start_row = i
                    break

        if start_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 44")

        data_rows = []
        data_start = start_row + 1
        for idx in range(data_start, len(df)):
            row = df.iloc[idx]
            if len(row) < 2 or pd.isna(row[1]) or str(row[1]).strip() == '':
                continue
            article = str(row[1]).strip()
            if 'итого' in article.lower() or 'всего' in article.lower():
                total_exp = self._clean_number(row[5]) if len(row) > 5 else 0
                data_rows.append({
                    'period': period,
                    'company': company,
                    'product_group': 'Расходы на продажу',
                    'nomenclature': 'Итого',
                    'revenue': 0,
                    'vat_in_revenue': 0,
                    'cost_price': 0,
                    'gross_profit': 0,
                    'sales_expenses': total_exp,
                    'other_income_expenses': 0,
                    'net_profit': 0,
                    'vat_deductible': 0,
                    'vat_to_budget': 0,
                    'quantity': 0
                })
                break
            else:
                expenses = self._clean_number(row[5]) if len(row) > 5 else 0
                if expenses != 0:
                    data_rows.append({
                        'period': period,
                        'company': company,
                        'product_group': 'Расходы на продажу',
                        'nomenclature': article,
                        'revenue': 0,
                        'vat_in_revenue': 0,
                        'cost_price': 0,
                        'gross_profit': 0,
                        'sales_expenses': expenses,
                        'other_income_expenses': 0,
                        'net_profit': 0,
                        'vat_deductible': 0,
                        'vat_to_budget': 0,
                        'quantity': 0
                    })
        if not data_rows:
            raise ValueError("Не удалось извлечь данные из ОСВ 44")
        df_result = pd.DataFrame(data_rows)
        return self.db.save_data(df_result)

    def _parse_osv_90(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._get_header_text(file_path, 5)
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        start_row = None
        for i in range(len(df)):
            cell_val = df.iloc[i, 0] if df.shape[1] > 0 else ''
            if pd.isna(cell_val):
                continue
            if 'счет' in str(cell_val).lower():
                cell_val2 = df.iloc[i, 1] if df.shape[1] > 1 else ''
                if not pd.isna(cell_val2) and 'показатели' in str(cell_val2).lower():
                    start_row = i
                    break

        if start_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 90")

        revenue = 0
        cost = 0
        vat = 0
        data_start = start_row + 1
        for idx in range(data_start, len(df)):
            row = df.iloc[idx]
            if len(row) == 0 or pd.isna(row[0]):
                continue
            account = str(row[0]).strip()
            if '90.01' in account:
                revenue = self._clean_number(row[5]) if len(row) > 5 else 0
            elif '90.02' in account:
                cost = self._clean_number(row[4]) if len(row) > 4 else 0
            elif '90.03' in account:
                vat = self._clean_number(row[4]) if len(row) > 4 else 0

        if revenue == 0 and cost == 0 and vat == 0:
            raise ValueError("Не удалось извлечь данные из ОСВ 90")
        gross = revenue - vat - cost
        data_row = {
            'period': period,
            'company': company,
            'product_group': 'Общие итоги',
            'nomenclature': 'ОСВ 90',
            'revenue': revenue,
            'vat_in_revenue': vat,
            'cost_price': cost,
            'gross_profit': gross,
            'sales_expenses': 0,
            'other_income_expenses': 0,
            'net_profit': gross,
            'vat_deductible': 0,
            'vat_to_budget': vat,
            'quantity': 0
        }
        df_result = pd.DataFrame([data_row])
        return self.db.save_data(df_result)

    def _parse_osv_91(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._get_header_text(file_path, 5)
        company = self._extract_company_from_text(header_text)
        period = self._extract_period_from_text(header_text, file_path)

        start_row = None
        for i in range(len(df)):
            cell_val = df.iloc[i, 0] if df.shape[1] > 0 else ''
            if pd.isna(cell_val):
                continue
            if 'счет' in str(cell_val).lower():
                cell_val2 = df.iloc[i, 1] if df.shape[1] > 1 else ''
                if not pd.isna(cell_val2) and 'прочие доходы и расходы' in str(cell_val2).lower():
                    start_row = i
                    break

        if start_row is None:
            raise ValueError("Не удалось найти заголовки в ОСВ 91")

        other_income = 0
        other_expense = 0
        data_start = start_row + 1
        for idx in range(data_start, len(df)):
            row = df.iloc[idx]
            if len(row) == 0 or pd.isna(row[0]):
                continue
            account = str(row[0]).strip()
            if '91.01' in account:
                other_income = self._clean_number(row[5]) if len(row) > 5 else 0
            elif '91.02' in account:
                other_expense = self._clean_number(row[4]) if len(row) > 4 else 0

        other_result = other_income - other_expense
        data_row = {
            'period': period,
            'company': company,
            'product_group': 'Прочие доходы/расходы',
            'nomenclature': 'ОСВ 91',
            'revenue': 0,
            'vat_in_revenue': 0,
            'cost_price': 0,
            'gross_profit': 0,
            'sales_expenses': 0,
            'other_income_expenses': other_result,
            'net_profit': 0,
            'vat_deductible': 0,
            'vat_to_budget': 0,
            'quantity': 0
        }
        df_result = pd.DataFrame([data_row])
        return self.db.save_data(df_result)

    def _parse_sales_report(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._get_header_text(file_path, 5)
        company = self._extract_company_from_text(header_text)
        period_base = self._extract_period_from_text(header_text, file_path)
        year = period_base.split('.')[1] if '.' in period_base else period_base

        start_row = None
        for i in range(len(df)):
            cell_val = df.iloc[i, 0] if df.shape[1] > 0 else ''
            if pd.isna(cell_val):
                continue
            if 'номенклатура' in str(cell_val).lower():
                start_row = i
                break

        if start_row is None:
            raise ValueError("Не удалось найти заголовок 'Номенклатура' в отчёте по продажам")

        months = []
        row = df.iloc[start_row]
        for col_idx, val in enumerate(row):
            if isinstance(val, str) and any(m in val.lower() for m in ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']):
                months.append((col_idx, val.strip()))
        if not months and start_row+1 < len(df):
            row = df.iloc[start_row+1]
            for col_idx, val in enumerate(row):
                if isinstance(val, str) and any(m in val.lower() for m in ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']):
                    months.append((col_idx, val.strip()))
            start_row += 1

        if not months:
            raise ValueError("Не удалось определить месяцы в отчёте по продажам")

        data_rows = []
        data_start = start_row + 2
        for idx in range(data_start, len(df)):
            row = df.iloc[idx]
            if pd.isna(row[0]) or str(row[0]).strip() == '':
                continue
            nomenclature = str(row[0]).strip()
            if 'итого' in nomenclature.lower() or 'всего' in nomenclature.lower():
                continue
            for (col_idx, month_name) in months:
                month_num = self._month_name_to_number(month_name)
                period = f"{month_num}.{year}"
                qty = self._clean_number(row[col_idx]) if len(row) > col_idx else 0
                amount = self._clean_number(row[col_idx+1]) if len(row) > col_idx+1 else 0
                if qty == 0 and amount == 0:
                    continue
                vat = amount * 20 / 120 if amount != 0 else 0
                data_rows.append({
                    'period': period,
                    'company': company,
                    'product_group': 'Товары',
                    'nomenclature': nomenclature,
                    'revenue': amount,
                    'vat_in_revenue': vat,
                    'cost_price': 0,
                    'gross_profit': 0,
                    'sales_expenses': 0,
                    'other_income_expenses': 0,
                    'net_profit': 0,
                    'vat_deductible': 0,
                    'vat_to_budget': vat,
                    'quantity': qty
                })
        if not data_rows:
            raise ValueError("Не удалось извлечь данные из отчёта по продажам")
        df_result = pd.DataFrame(data_rows)
        return self.db.save_data(df_result)

    def _month_name_to_number(self, month_name):
        """Преобразует русское название месяца в номер"""
        month_names = {
            'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04', 'май': '05', 'июн': '06',
            'июл': '07', 'авг': '08', 'сен': '09', 'окт': '10', 'ноя': '11', 'дек': '12'
        }
        for key, num in month_names.items():
            if key in month_name.lower():
                return num
        return '01'
    
    def _flatten_text(self, df, rows):
        """Преобразует указанные строки DataFrame в одну строку текста"""
        if isinstance(rows, slice):
            subset = df.iloc[rows]
        else:
            # Если передан список индексов строк
            subset = df.iloc[list(rows)]
        # Заменяем NaN на пустую строку
        subset = subset.fillna('')
        # Преобразуем каждое значение в строку вручную
        strings = []
        for _, row in subset.iterrows():
            for cell in row:
                strings.append(str(cell))
        return ' '.join(strings)

    def _row_to_text(self, row):
        """Преобразует одну строку Series в текст"""
        # row - это pandas Series
        strings = []
        for cell in row:
            if pd.isna(cell):
                strings.append('')
            else:
                strings.append(str(cell))
        return ' '.join(strings)

    def _import_legacy(self, file_path):
        """Старая логика импорта сводного файла (с русскими колонками) - оставлена для совместимости"""
        df = pd.read_excel(file_path)
        column_mapping = {
            'Период': 'period',
            'Компания': 'company',
            'Товарная группа': 'product_group',
            'Номенклатура': 'nomenclature',
            'Выручка (с НДС)': 'revenue',
            'Выручка': 'revenue',
            'НДС в выручке': 'vat_in_revenue',
            'Себестоимость': 'cost_price',
            'Валовая прибыль': 'gross_profit',
            'Расходы на продажу': 'sales_expenses',
            'Прочие доходы/расходы': 'other_income_expenses',
            'Чистая прибыль': 'net_profit',
            'НДС к вычету': 'vat_deductible',
            'НДС К УПЛАТЕ': 'vat_to_budget',
            'НДС к уплате': 'vat_to_budget',
            'Оборот (кол-во)': 'quantity',
            'Количество': 'quantity'
        }
        df.rename(columns=lambda x: column_mapping.get(str(x).strip(), str(x).strip()), inplace=True)
        required = ['period', 'company', 'product_group', 'nomenclature', 'revenue',
                    'vat_in_revenue', 'cost_price', 'vat_to_budget', 'quantity']
        missing = [c for c in required if c not in df.columns]
        if missing:
            ru_names = {'period':'Период','company':'Компания','product_group':'Товарная группа',
                        'nomenclature':'Номенклатура','revenue':'Выручка (с НДС)','vat_in_revenue':'НДС в выручке',
                        'cost_price':'Себестоимость','vat_to_budget':'НДС к уплате','quantity':'Количество'}
            missing_ru = [ru_names.get(c,c) for c in missing]
            raise ValueError(
                f"Файл не является сводным отчётом.\n"
                f"Отсутствуют колонки: {', '.join(missing_ru)}\n"
                "Используйте кнопку «Скачать шаблон» для подготовки данных."
            )
        if 'gross_profit' not in df.columns:
            df['gross_profit'] = df['revenue'] - df['vat_in_revenue'] - df['cost_price']
        if 'net_profit' not in df.columns:
            df['net_profit'] = df['gross_profit']
            if 'sales_expenses' in df.columns:
                df['net_profit'] -= df['sales_expenses']
            if 'other_income_expenses' in df.columns:
                df['net_profit'] += df['other_income_expenses']
        for col in ['sales_expenses','other_income_expenses','vat_deductible']:
            if col not in df.columns:
                df[col] = 0
        return self.db.save_data(df)    
   
    def _get_russian_column_name(self, eng_name):
        """Возвращает русское название колонки по английскому"""
        ru_names = {
            'period': 'Период',
            'company': 'Компания',
            'product_group': 'Товарная группа',
            'nomenclature': 'Номенклатура',
            'revenue': 'Выручка (с НДС)',
            'vat_in_revenue': 'НДС в выручке',
            'cost_price': 'Себестоимость',
            'vat_to_budget': 'НДС к уплате',
            'quantity': 'Количество'
        }
        return ru_names.get(eng_name, eng_name)

    def load_single_excel(self, file_path=None):
        """
        Загрузка одного файла с диалогом (для совместимости).
        Если file_path не передан, открывает диалог выбора файла.
        Показывает сообщения об успехе/ошибке.
        """
        if file_path is None:
            file_path, _ = QFileDialog.getOpenFileName(
                self, "Выберите файл Excel", "", "Excel Files (*.xlsx *.xls)"
            )
            if not file_path:
                return

        try:
            records_count = self._import_excel_file(file_path)
            # Обновляем отображение после успешной загрузки
            self.current_df = self.db.get_all_data()  # или применить текущие фильтры
            self.display_data(self.current_df)
            self.update_totals()
            self.update_charts()
            QMessageBox.information(
                self, "Успех",
                f"Загружено {records_count} записей из файла: {os.path.basename(file_path)}"
            )
        except Exception as e:
            QMessageBox.critical(
                self, "Ошибка",
                f"Ошибка при загрузке файла {os.path.basename(file_path)}:\n{str(e)}"
            )
    
    def display_data(self, df):
        """Отображение данных в таблице"""
        self.table_model.setRowCount(0)
        
        for index, row in df.iterrows():
            items = []
            for col in df.columns:
                value = row[col]
                if isinstance(value, (int, float)):
                    # Форматирование чисел с разделителями тысяч
                    if col in ['revenue', 'vat_in_revenue', 'cost_price', 'gross_profit', 
                              'sales_expenses', 'other_income_expenses', 'net_profit',
                              'vat_deductible', 'vat_to_budget']:
                        display_value = f"{value:,.0f} ₽".replace(",", " ")
                    else:
                        display_value = str(value)
                else:
                    display_value = str(value)
                
                item = QStandardItem(display_value)
                item.setData(value)  # Сохраняем исходное значение для сортировки
                
                # Цветовое выделение для НДС к уплате
                if col == 'vat_to_budget' and isinstance(value, (int, float)):
                    if value > 100000:
                        item.setBackground(QColor(255, 200, 200))  # Красный для больших сумм
                    elif value < 0:
                        item.setBackground(QColor(200, 255, 200))  # Зеленый для возврата
                
                items.append(item)
            
            self.table_model.appendRow(items)
    
    def apply_filters(self):
        """Применение фильтров"""
        company = self.company_combo.currentText()
        period = self.period_combo.currentText()
        product_group = self.group_combo.currentText()
        
        filtered_df = self.db.get_filtered_data(
            company if company != "Все компании" else None,
            period if period != "Все периоды" else None,
            product_group if product_group != "Все группы" else None
        )
        
        if not filtered_df.empty:
            self.current_df = filtered_df
            self.display_data(filtered_df)
            self.update_totals()
             # === ДОБАВЬТЕ ЭТУ СТРОКУ ДЛЯ ОБНОВЛЕНИЯ ГРАФИКОВ ===
            self.update_charts()
    
    def update_totals(self):
        if self.current_df is not None and not self.current_df.empty:
            # Принудительно конвертируем числовые колонки в float
            for col in ['revenue', 'vat_to_budget', 'net_profit']:
                if col in self.current_df.columns:
                    self.current_df[col] = pd.to_numeric(self.current_df[col], errors='coerce').fillna(0)
            total_revenue = self.current_df['revenue'].sum()
            total_vat = self.current_df['vat_to_budget'].sum()
            total_profit = self.current_df['net_profit'].sum()
            self.revenue_label.setText(f"Выручка: {total_revenue:,.0f} ₽".replace(",", " "))
            self.vat_label.setText(f"НДС к уплате: {total_vat:,.0f} ₽".replace(",", " "))
        self.profit_label.setText(f"Чистая прибыль: {total_profit:,.0f} ₽".replace(",", " "))
    
    def update_charts(self):
        if self.current_df is None or self.current_df.empty:
            # Если данных нет, очищаем графики и выводим сообщение
            for ax in self.axes.flat:
                ax.clear()
                ax.text(0.5, 0.5, 'Нет данных для отображения', 
                        ha='center', va='center', fontsize=12)
            self.canvas.draw()
            return

        # Заменяем NaN на 0 для числовых колонок
        df_clean = self.current_df.fillna(0)

        # Очистка предыдущих графиков
        for ax in self.axes.flat:
            ax.clear()

        # 1. Круговая диаграмма по товарным группам
        try:
            if 'product_group' in df_clean.columns and not df_clean['product_group'].empty:
                group_profit = df_clean.groupby('product_group')['net_profit'].sum()
                if not group_profit.empty and group_profit.sum() != 0:
                    colors1 = plt.cm.Set3(np.linspace(0, 1, len(group_profit)))
                    self.axes[0, 0].pie(group_profit.values, labels=group_profit.index, 
                                        autopct='%1.1f%%', colors=colors1, startangle=90)
                    self.axes[0, 0].set_title('Распределение прибыли по товарным группам')
                else:
                    self.axes[0, 0].text(0.5, 0.5, 'Нет данных по группам', 
                                        ha='center', va='center')
            else:
                self.axes[0, 0].text(0.5, 0.5, 'Нет данных по группам', 
                                    ha='center', va='center')
        except Exception as e:
            print(f"Ошибка при построении круговой диаграммы: {e}")
            self.axes[0, 0].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 2. Столбчатая диаграмма НДС по компаниям
        try:
            if 'company' in df_clean.columns and not df_clean['company'].empty:
                company_vat = df_clean.groupby('company')['vat_to_budget'].sum()
                if not company_vat.empty and company_vat.sum() != 0:
                    bars = self.axes[0, 1].bar(company_vat.index, company_vat.values,
                                            color=['#3498db', '#2ecc71', '#e74c3c'])
                    self.axes[0, 1].set_title('НДС к уплате по компаниям')
                    self.axes[0, 1].set_ylabel('Сумма НДС, ₽')
                    self.axes[0, 1].tick_params(axis='x', rotation=45)
                    # Добавление значений над столбцами
                    for bar in bars:
                        height = bar.get_height()
                        if height > 0:
                            self.axes[0, 1].text(bar.get_x() + bar.get_width()/2., height,
                                                f'{height:,.0f}'.replace(",", " "),
                                                ha='center', va='bottom', fontsize=8)
                else:
                    self.axes[0, 1].text(0.5, 0.5, 'Нет данных по компаниям',
                                        ha='center', va='center')
            else:
                self.axes[0, 1].text(0.5, 0.5, 'Нет данных по компаниям',
                                    ha='center', va='center')
        except Exception as e:
            print(f"Ошибка при построении столбчатой диаграммы: {e}")
            self.axes[0, 1].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 3. Линейный график выручки по периодам
        try:
            if 'period' in df_clean.columns and not df_clean['period'].empty:
                period_revenue = df_clean.groupby('period')['revenue'].sum().sort_index()
                if not period_revenue.empty and period_revenue.sum() != 0:
                    self.axes[1, 0].plot(period_revenue.index, period_revenue.values,
                                        marker='o', linewidth=2, color='#9b59b6')
                    self.axes[1, 0].set_title('Динамика выручки по периодам')
                    self.axes[1, 0].set_ylabel('Выручка, ₽')
                    self.axes[1, 0].grid(True, alpha=0.3)
                    self.axes[1, 0].tick_params(axis='x', rotation=45)
                else:
                    self.axes[1, 0].text(0.5, 0.5, 'Нет данных по периодам',
                                        ha='center', va='center')
            else:
                self.axes[1, 0].text(0.5, 0.5, 'Нет данных по периодам',
                                    ha='center', va='center')
        except Exception as e:
            print(f"Ошибка при построении линейного графика: {e}")
            self.axes[1, 0].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 4. ТОП-5 товаров по прибыльности
        try:
            if 'nomenclature' in df_clean.columns and not df_clean['nomenclature'].empty:
                top_products = df_clean.nlargest(5, 'net_profit')[['nomenclature', 'net_profit']]
                if not top_products.empty and top_products['net_profit'].sum() > 0:
                    # Ограничим длину названий
                    labels = [str(x)[:20] + '...' if len(str(x)) > 20 else str(x) 
                            for x in top_products['nomenclature']]
                    bars = self.axes[1, 1].barh(labels, top_products['net_profit'],
                                            color=plt.cm.viridis(np.linspace(0.2, 0.8, len(top_products))))
                    self.axes[1, 1].set_title('ТОП-5 товаров по прибыльности')
                    self.axes[1, 1].set_xlabel('Прибыль, ₽')
                else:
                    self.axes[1, 1].text(0.5, 0.5, 'Нет данных по товарам',
                                        ha='center', va='center')
            else:
                self.axes[1, 1].text(0.5, 0.5, 'Нет данных по товарам',
                                    ha='center', va='center')
        except Exception as e:
            print(f"Ошибка при построении ТОП-5: {e}")
            self.axes[1, 1].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # Автонастройка макета с защитой от ошибок
        try:
            plt.tight_layout()
        except Exception as e:
            print(f"Ошибка tight_layout: {e}")
        self.canvas.draw()
    
    def export_to_excel(self):
        """Экспорт данных в Excel с графиками"""
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Предупреждение", "Нет данных для экспорта")
            return
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить как Excel", "отчет_buh_tuund.xlsx", "Excel Files (*.xlsx)"
        )
        
        if file_path:
            try:
                # Сохранение графика в буфер
                buf = io.BytesIO()
                self.figure.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                buf.seek(0)
                
                # Создание Excel файла
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    # Основные данные
                    self.current_df.to_excel(writer, sheet_name='Данные', index=False)
                    
                    # Сводная информация
                    summary_df = pd.DataFrame({
                        'Показатель': ['Общая выручка', 'Общий НДС к уплате', 'Общая прибыль', 
                                      'Количество записей', 'Дата экспорта'],
                        'Значение': [
                            f"{self.current_df['revenue'].sum():,.0f} ₽".replace(",", " "),
                            f"{self.current_df['vat_to_budget'].sum():,.0f} ₽".replace(",", " "),
                            f"{self.current_df['net_profit'].sum():,.0f} ₽".replace(",", " "),
                            len(self.current_df),
                            datetime.now().strftime("%d.%m.%Y %H:%M")
                        ]
                    })
                    summary_df.to_excel(writer, sheet_name='Итоги', index=False)
                    
                    # Настройка ширины колонок и стилей
                    workbook = writer.book
                    for sheet_name in workbook.sheetnames:
                        worksheet = workbook[sheet_name]
                        
                        # Автоширина колонок
                        for column in worksheet.columns:
                            max_length = 0
                            column_letter = column[0].column_letter
                            for cell in column:
                                try:
                                    if len(str(cell.value)) > max_length:
                                        max_length = len(str(cell.value))
                                except:
                                    pass
                            adjusted_width = min(max_length + 2, 50)
                            worksheet.column_dimensions[column_letter].width = adjusted_width
                        
                        # Жирный заголовок
                        for cell in worksheet[1]:
                            cell.font = Font(bold=True)
                
                QMessageBox.information(self, "Успех", f"Файл сохранен: {file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Ошибка при экспорте: {str(e)}")
    
    def export_to_pdf(self):
        """Экспорт отчета в PDF с поддержкой кириллицы"""
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Предупреждение", "Нет данных для экспорта")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить как PDF", "отчет_buh_tuund.pdf", "PDF Files (*.pdf)"
        )

        if not file_path:
            return

        try:
            # --- Регистрация шрифта с поддержкой кириллицы ---
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.enums import TA_CENTER
            from reportlab.lib import colors
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
            from reportlab.lib.pagesizes import A4

            # Регистрируем шрифт Arial
            pdfmetrics.registerFont(TTFont('Arial', 'arial.ttf'))

            doc = SimpleDocTemplate(file_path, pagesize=A4)
            elements = []
            styles = getSampleStyleSheet()

            # Устанавливаем Arial для всех стандартных стилей
            for style_name in styles.byName:
                styles[style_name].fontName = 'Arial'

            # Стиль для заголовка
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontName='Arial',
                fontSize=16,
                alignment=TA_CENTER,
                spaceAfter=20,
                textColor=colors.HexColor('#2c3e50')
            )

            # --- Заголовок ---
            elements.append(Paragraph("БУХГАЛТЕРСКИЙ ОТЧЕТ BUHTUUNDOTCHET", title_style))

            # --- Информация ---
            info_text = f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M')} | Записей: {len(self.current_df)}"
            elements.append(Paragraph(info_text, styles['Normal']))
            elements.append(Spacer(1, 20))

            # --- Итоговые показатели (каждый отдельным абзацем) ---
            total_revenue = self.current_df['revenue'].sum()
            total_vat = self.current_df['vat_to_budget'].sum()
            total_profit = self.current_df['net_profit'].sum()

            elements.append(Paragraph("<b>ИТОГОВЫЕ ПОКАЗАТЕЛИ:</b>", styles['Heading2']))
            elements.append(Spacer(1, 6))
            elements.append(Paragraph(f"Общая выручка: {total_revenue:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"НДС к уплате в бюджет: {total_vat:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"Общая чистая прибыль: {total_profit:,.0f} ₽", styles['Normal']))
            elements.append(Spacer(1, 20))

            # --- График ---
            chart_path = "temp_chart.png"
            self.figure.savefig(chart_path, format='png', dpi=150, bbox_inches='tight')
            elements.append(Paragraph("Визуализация данных:", styles['Heading2']))
            elements.append(Image(chart_path, width=400, height=300))
            elements.append(Spacer(1, 20))

            # --- Таблица (первые 20 строк) ---
            elements.append(Paragraph("Данные отчета (первые 20 записей):", styles['Heading2']))

            table_data = [['Период', 'Компания', 'Товар', 'Выручка', 'НДС к уплате', 'Прибыль']]
            for _, row in self.current_df.head(20).iterrows():
                table_data.append([
                    str(row.get('period', '')),
                    str(row.get('company', '')),
                    str(row.get('nomenclature', ''))[:20],
                    f"{row.get('revenue', 0):,.0f} ₽".replace(",", " "),
                    f"{row.get('vat_to_budget', 0):,.0f} ₽".replace(",", " "),
                    f"{row.get('net_profit', 0):,.0f} ₽".replace(",", " ")
                ])

            table = Table(table_data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Arial'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('FONTNAME', (0, 1), (-1, -1), 'Arial'),
            ]))
            elements.append(table)
            elements.append(Spacer(1, 20))

            # --- Подпись ---
            footer_style = ParagraphStyle(
                'Footer',
                parent=styles['Italic'],
                fontName='Arial',
                fontSize=8,
                alignment=TA_CENTER,
                textColor=colors.grey
            )
            elements.append(Paragraph("Сформировано программой BuhTuundOtchet v1.0", footer_style))

            # Генерация PDF
            doc.build(elements)

            # Удаление временного файла
            if os.path.exists(chart_path):
                os.remove(chart_path)

            QMessageBox.information(self, "Успех", f"PDF файл сохранен: {file_path}")

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при экспорте в PDF: {str(e)}")
    
    
    
    def export_to_word(self):
        """Экспорт отчета в Word"""
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Предупреждение", "Нет данных для экспорта")
            return
        
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить как Word", "отчет_buh_tuund.docx", "Word Files (*.docx)"
        )
        
        if file_path:
            try:
                # Создание документа Word
                doc = docx.Document()
                
                # Заголовок
                title = doc.add_heading('БУХГАЛТЕРСКИЙ ОТЧЕТ BUHTUUNDOTCHET', 0)
                title.alignment = docx.enum.text.WD_ALIGN_PARAGRAPH.CENTER
                
                # Информация о отчете
                doc.add_paragraph(f'Дата формирования: {datetime.now().strftime("%d.%m.%Y %H:%M")}')
                doc.add_paragraph(f'Количество записей: {len(self.current_df)}')
                doc.add_paragraph()
                
                # Итоговые показатели
                total_revenue = self.current_df['revenue'].sum()
                total_vat = self.current_df['vat_to_budget'].sum()
                total_profit = self.current_df['net_profit'].sum()
                
                totals_para = doc.add_paragraph()
                totals_para.add_run('ИТОГОВЫЕ ПОКАЗАТЕЛИ:\n').bold = True
                totals_para.add_run(f'Общая выручка: {total_revenue:,.0f} ₽\n'.replace(",", " "))
                totals_para.add_run(f'НДС к уплате в бюджет: {total_vat:,.0f} ₽\n'.replace(",", " "))
                totals_para.add_run(f'Общая чистая прибыль: {total_profit:,.0f} ₽'.replace(",", " "))
                
                doc.add_paragraph()
                
                # Сохранение графика и вставка в документ
                chart_path = "temp_chart_word.png"
                self.figure.savefig(chart_path, format='png', dpi=150, bbox_inches='tight')
                
                doc.add_heading('Визуализация данных:', level=2)
                doc.add_picture(chart_path, width=Inches(6))
                doc.add_paragraph()
                
                # Таблица с данными
                doc.add_heading('Данные отчета (первые 15 записей):', level=2)
                
                # Создание таблицы
                table = doc.add_table(rows=1, cols=6)
                table.style = 'LightShading-Accent1'
                
                # Заголовки таблицы
                headers = ['Период', 'Компания', 'Товар', 'Выручка', 'НДС к уплате', 'Прибыль']
                for i, header in enumerate(headers):
                    table.cell(0, i).text = header
                    table.cell(0, i).paragraphs[0].runs[0].bold = True
                
                # Заполнение таблицы данными
                for _, row in self.current_df.head(15).iterrows():
                    cells = table.add_row().cells
                    cells[0].text = str(row.get('period', ''))
                    cells[1].text = str(row.get('company', ''))
                    cells[2].text = str(row.get('nomenclature', ''))[:20]
                    cells[3].text = f"{row.get('revenue', 0):,.0f} ₽".replace(",", " ")
                    cells[4].text = f"{row.get('vat_to_budget', 0):,.0f} ₽".replace(",", " ")
                    cells[5].text = f"{row.get('net_profit', 0):,.0f} ₽".replace(",", " ")
                
                doc.add_paragraph()
                doc.add_paragraph('Сформировано программой BuhTuundOtchet v1.0').italic = True
                
                # Сохранение документа
                doc.save(file_path)
                
                # Удаление временного файла
                if os.path.exists(chart_path):
                    os.remove(chart_path)
                
                QMessageBox.information(self, "Успех", f"Word файл сохранен: {file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Ошибка при экспорте в Word: {str(e)}")
    
    def generate_quick_report(self):
        """Генерация быстрого отчета"""
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Предупреждение", "Нет данных для отчета")
            return
        
        # Расчет основных показателей
        total_revenue = self.current_df['revenue'].sum()
        total_vat = self.current_df['vat_to_budget'].sum()
        total_profit = self.current_df['net_profit'].sum()
        
        # Топ товаров
        top_products = self.current_df.nlargest(5, 'net_profit')[['nomenclature', 'net_profit']]
        top_products_text = "\n".join([f"{row['nomenclature']}: {row['net_profit']:,.0f} ₽" 
                                      for _, row in top_products.iterrows()])
        
        # Сообщение с отчетом
        report_text = f"""
        <h3>БЫСТРЫЙ ОТЧЕТ BUHTUUNDOTCHET</h3>
        <p><b>Период анализа:</b> {self.period_combo.currentText()}</p>
        <p><b>Компания:</b> {self.company_combo.currentText()}</p>
        <hr>
        <p><b>ОСНОВНЫЕ ПОКАЗАТЕЛИ:</b></p>
        <p>• Общая выручка: <span style='color: #27ae60; font-weight: bold;'>{total_revenue:,.0f} ₽</span></p>
        <p>• НДС к уплате в бюджет: <span style='color: #e74c3c; font-weight: bold;'>{total_vat:,.0f} ₽</span></p>
        <p>• Чистая прибыль: <span style='color: #3498db; font-weight: bold;'>{total_profit:,.0f} ₽</span></p>
        <hr>
        <p><b>ТОП-5 товаров по прибыльности:</b></p>
        <pre>{top_products_text}</pre>
        <hr>
        <p><i>Сформировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}</i></p>
        """
        
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Быстрый отчет")
        msg_box.setTextFormat(Qt.TextFormat.RichText)
        msg_box.setText(report_text)
        msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg_box.exec()
    
    def show_about(self):
        """Показывает окно 'О программе'"""
        about_text = """<h2>Программа BuhTuundOtchet</h2>
        <p><b>Версия программы:</b> v2.0.3</p>
        <p><b>Разработчик:</b> Deer Tuund (C) 2026</p>
        <p><b>Для связи:</b> vaspull9@gmail.com</p>
        <hr>
        <p>Программа для разработки отчетов из 1С</p>
        <p><b>Возможности:</b></p>
        <ul>
            <li>Импорт данных из Excel (выгрузок 1С)</li>
            <li>Хранение данных в SQLite базе</li>
            <li>Фильтрация по компаниям, периодам, товарным группам</li>
            <li>Расчет валовой и чистой прибыли</li>
            <li>Акцент на расчете НДС к уплате в бюджет</li>
            <li>Визуализация данных (графики и диаграммы)</li>
            <li>Экспорт в Excel, PDF, Word</li>
            <li>Современный интерфейс с темной темой</li>
        </ul>
        <p><b>Используемые технологии:</b> Python, PyQt6, Pandas, Matplotlib, SQLite, ReportLab</p>
        """
        
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("О программе BuhTuundOtchet")
        msg_box.setTextFormat(Qt.TextFormat.RichText)
        msg_box.setText(about_text)
        msg_box.setIconPixmap(QPixmap(64, 64))
        msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg_box.exec()

# ==================== ЗАПУСК ПРОГРАММЫ ====================
def main():
    app = QApplication(sys.argv)
    
    # Установка стиля Fusion для современного вида
    app.setStyle('Fusion')
    
    # Иконка приложения
    app.setWindowIcon(QIcon.fromTheme("office-chart-line"))
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())

if __name__ == '__main__':
    main()