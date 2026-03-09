#======================================================================
# BuhTuundOtchet v7.1.0 - работают книги покупок и продаж, новые меню
import sys
import os
import sqlite3
import re
import shutil
import io
import calendar
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
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import docx
from docx.shared import Inches, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
import openpyxl
from openpyxl.styles import Font, Alignment
from openpyxl.drawing.image import Image as ExcelImage

# ==================== БАЗА ДАННЫХ ====================
class DatabaseManager:
    def __init__(self, db_path='buh_tuund.db'):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.create_tables()
        
    def create_tables(self):
        cursor = self.conn.cursor()

        # Основная таблица reports
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company TEXT,
                period_start TEXT,
                period_end TEXT,
                doc_type TEXT,
                product_group TEXT,
                nomenclature TEXT,
                revenue REAL,
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

        # Добавляем новые колонки, если их нет
        cursor.execute("PRAGMA table_info(reports)")
        existing = [col[1] for col in cursor.fetchall()]
        
        new_columns = {
            'seller': 'TEXT',
            'buyer': 'TEXT',
            'document_number': 'TEXT',
            'document_date': 'TEXT',
            'operation_code': 'TEXT',
            'acceptance_date': 'TEXT',
            'payment_document': 'TEXT',
            'purchase_amount_with_vat': 'REAL',
            'sales_amount_without_vat': 'REAL',
            'sales_amount_with_vat': 'REAL'
        }
        
        for col, typ in new_columns.items():
            if col not in existing:
                cursor.execute(f"ALTER TABLE reports ADD COLUMN {col} {typ}")

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
        """Сохраняет данные из DataFrame в таблицу reports"""
        df_to_save = df.copy()

        # Все возможные колонки со значениями по умолчанию
        all_columns = {
            'company': '',
            'period_start': '',
            'period_end': '',
            'doc_type': '',
            'product_group': '',
            'nomenclature': '',
            'revenue': 0.0,
            'cost_price': 0.0,
            'gross_profit': 0.0,
            'sales_expenses': 0.0,
            'other_income_expenses': 0.0,
            'net_profit': 0.0,
            'vat_deductible': 0.0,
            'vat_to_budget': 0.0,
            'quantity': 0,
            'seller': '',
            'buyer': '',
            'document_number': '',
            'document_date': '',
            'operation_code': '',
            'acceptance_date': '',
            'payment_document': '',
            'purchase_amount_with_vat': 0.0,
            'sales_amount_without_vat': 0.0,
            'sales_amount_with_vat': 0.0
        }

        for col, default in all_columns.items():
            if col not in df_to_save.columns:
                df_to_save[col] = default

        # Числовые колонки
        numeric_cols = [
            'revenue', 'cost_price', 'gross_profit', 'sales_expenses',
            'other_income_expenses', 'net_profit', 'vat_deductible', 'vat_to_budget',
            'purchase_amount_with_vat', 'sales_amount_without_vat', 'sales_amount_with_vat'
        ]
        for col in numeric_cols:
            if col in df_to_save.columns:
                df_to_save[col] = pd.to_numeric(df_to_save[col], errors='coerce').fillna(0.0)

        if 'quantity' in df_to_save.columns:
            df_to_save['quantity'] = pd.to_numeric(df_to_save['quantity'], errors='coerce').fillna(0).astype(int)

        if 'id' in df_to_save.columns:
            df_to_save = df_to_save.drop(columns=['id'])

        df_to_save.to_sql('reports', self.conn, if_exists='append', index=False)
        self.conn.commit()
        return len(df_to_save)

    def get_all_data(self):
        query = "SELECT * FROM reports ORDER BY period_start DESC, company"
        return pd.read_sql_query(query, self.conn)

    def get_filtered_data(self, company=None, date_from=None, date_to=None, product_group=None, doc_type=None):
        query = "SELECT * FROM reports WHERE 1=1"
        params = []

        if company and company != "Все компании":
            query += " AND company = ?"
            params.append(company)

        if date_from:
            query += " AND period_start >= ?"
            params.append(date_from)

        if date_to:
            query += " AND period_end <= ?"
            params.append(date_to)

        if product_group and product_group != "Все группы":
            query += " AND product_group = ?"
            params.append(product_group)

        if doc_type:
            query += " AND doc_type = ?"
            params.append(doc_type)

        query += " ORDER BY period_start DESC, company"
        return pd.read_sql_query(query, self.conn, params=params)


# ==================== ГЛАВНОЕ ОКНО ====================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = DatabaseManager()
        self.current_df = None
        self.settings = QSettings("DeerTuund", "BuhTuundOtchet")
        
        # Пути из настроек
        self.load_folder = self.settings.value("load_folder", "")
        self.save_folder = self.settings.value("save_folder", "")
        self.db_load_folder = self.settings.value("db_load_folder", "")
        self.db_save_folder = self.settings.value("db_save_folder", "")
        
        self.init_ui()
        self.load_last_database()

    # ==================== НАСТРОЙКИ ====================
    def load_settings(self):
        self.load_folder = self.settings.value("load_folder", "")
        self.save_folder = self.settings.value("save_folder", "")
        self.db_load_folder = self.settings.value("db_load_folder", "")
        self.db_save_folder = self.settings.value("db_save_folder", "")

    def save_settings(self):
        self.settings.setValue("load_folder", self.load_folder)
        self.settings.setValue("save_folder", self.save_folder)
        self.settings.setValue("db_load_folder", self.db_load_folder)
        self.settings.setValue("db_save_folder", self.db_save_folder)

    def show_settings(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("Настройки")
        dialog.setModal(True)
        layout = QVBoxLayout(dialog)

        # Папка загрузки данных
        load_layout = QHBoxLayout()
        load_layout.addWidget(QLabel("Папка загрузки данных:"))
        self.load_folder_edit = QLineEdit(self.load_folder)
        load_layout.addWidget(self.load_folder_edit)
        load_btn = QPushButton("Обзор...")
        load_btn.clicked.connect(lambda: self._choose_folder(self.load_folder_edit, "load_folder"))
        load_layout.addWidget(load_btn)
        layout.addLayout(load_layout)

        # Папка сохранения отчетов
        save_layout = QHBoxLayout()
        save_layout.addWidget(QLabel("Папка сохранения отчетов:"))
        self.save_folder_edit = QLineEdit(self.save_folder)
        save_layout.addWidget(self.save_folder_edit)
        save_btn = QPushButton("Обзор...")
        save_btn.clicked.connect(lambda: self._choose_folder(self.save_folder_edit, "save_folder"))
        save_layout.addWidget(save_btn)
        layout.addLayout(save_layout)

        # Папка загрузки БД
        db_load_layout = QHBoxLayout()
        db_load_layout.addWidget(QLabel("Папка загрузки БД:"))
        self.db_load_folder_edit = QLineEdit(self.db_load_folder)
        db_load_layout.addWidget(self.db_load_folder_edit)
        db_load_btn = QPushButton("Обзор...")
        db_load_btn.clicked.connect(lambda: self._choose_folder(self.db_load_folder_edit, "db_load_folder"))
        db_load_layout.addWidget(db_load_btn)
        layout.addLayout(db_load_layout)

        # Папка сохранения БД
        db_save_layout = QHBoxLayout()
        db_save_layout.addWidget(QLabel("Папка сохранения БД:"))
        self.db_save_folder_edit = QLineEdit(self.db_save_folder)
        db_save_layout.addWidget(self.db_save_folder_edit)
        db_save_btn = QPushButton("Обзор...")
        db_save_btn.clicked.connect(lambda: self._choose_folder(self.db_save_folder_edit, "db_save_folder"))
        db_save_layout.addWidget(db_save_btn)
        layout.addLayout(db_save_layout)

        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box.accepted.connect(lambda: self._save_settings_from_dialog(dialog))
        btn_box.rejected.connect(dialog.reject)
        layout.addWidget(btn_box)

        dialog.exec()

    def _choose_folder(self, line_edit, setting_key):
        folder = QFileDialog.getExistingDirectory(self, "Выберите папку")
        if folder:
            line_edit.setText(folder)
            setattr(self, setting_key, folder)

    def _save_settings_from_dialog(self, dialog):
        self.load_folder = self.load_folder_edit.text()
        self.save_folder = self.save_folder_edit.text()
        self.db_load_folder = self.db_load_folder_edit.text()
        self.db_save_folder = self.db_save_folder_edit.text()
        self.save_settings()
        dialog.accept()

    # ==================== ЗАГРУЗКА ПОСЛЕДНЕЙ БД ====================
    def load_last_database(self):
        last_db = self.settings.value("last_database", "")
        if last_db and os.path.exists(last_db):
            try:
                self.db.conn.close()
                self.db = DatabaseManager(db_path=last_db)
                self.current_df = self.db.get_all_data()
                self.display_data(self.current_df)
                self.update_summary()
                self.update_charts()
                self.update_filter_combos()
                print(f"Загружена последняя БД: {last_db}")
            except Exception as e:
                print(f"Не удалось загрузить последнюю БД: {e}")

    # ==================== ИНИЦИАЛИЗАЦИЯ ИНТЕРФЕЙСА ====================
    def init_ui(self):
        self.setWindowTitle("BuhTuundOtchet v7.0.1")
        self.setGeometry(100, 100, 1400, 800)
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QMenuBar {
                background-color: #2c3e50;
                color: white;
                font-weight: bold;
            }
            QMenuBar::item {
                background-color: #2c3e50;
                color: white;
                padding: 5px 10px;
            }
            QMenuBar::item:selected {
                background-color: #3498db;
            }
            QMenu {
                background-color: #ecf0f1;
                border: 1px solid #bdc3c7;
            }
            QMenu::item:selected {
                background-color: #3498db;
                color: white;
            }
            QToolBar {
                background-color: #34495e;
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

        self.create_menus()

        # Центральный виджет - ОБЯЗАТЕЛЬНО!!!
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Панель фильтров
        filter_layout = QHBoxLayout()
        
        self.company_combo = QComboBox()
        self.company_combo.addItems(["Все компании"])
        
        self.period_combo = QComboBox()
        self.period_combo.addItems(["Все периоды"])
        
        self.group_combo = QComboBox()
        self.group_combo.addItems(["Все группы"])
        
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
        
        # ДОБАВЛЯЕМ ФИЛЬТРЫ В MAIN_LAYOUT
        main_layout.addLayout(filter_layout)

        # СОЗДАЁМ СПЛИТТЕР - ОН БУДЕТ РАЗДЕЛЯТЬ ЛЕВУЮ И ПРАВУЮ ПАНЕЛИ
        self.splitter = QSplitter(Qt.Orientation.Horizontal)

        # Левая панель с деревом
        self.left_panel = QWidget()
        left_layout = QVBoxLayout(self.left_panel)
        left_layout.setContentsMargins(2, 2, 2, 2)

        self.select_root_btn = QPushButton("Выбрать папку...")
        self.select_root_btn.clicked.connect(self.choose_root_folder)
        left_layout.addWidget(self.select_root_btn)

        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderHidden(True)
        self.tree_widget.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left_layout.addWidget(self.tree_widget)

        self.tree_widget.itemChanged.connect(self._handle_item_changed)
        self.tree_widget.itemChanged.connect(self._update_process_button_state)

        self.splitter.addWidget(self.left_panel)

        # Правая панель с вкладками
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)

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

        self.table_view = QTableView()
        self.table_model = QStandardItemModel()
        self.table_view.setModel(self.table_model)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(True)

        headers = [
            "ID", "Компания", "Период с", "Период по", "Тип", "Группа",
            "Продавец", "Покупатель", "Номенклатура",
            "№ сч/ф", "Дата сч/ф", "Код опер.", "Дата принятия", "Плат. док.",
            "Сумма покупки с НДС", "Сумма продажи без НДС", "Сумма продажи с НДС",
            "Выручка", "Себестоимость", "Валовая прибыль",
            "Расходы на продажу", "Прочие доходы/расходы", "Чистая прибыль",
            "НДС покупки", "НДС продажи", "Кол-во", "Дата импорта"
        ]
        self.table_model.setHorizontalHeaderLabels(headers)

        table_layout.addWidget(self.table_view)

        # Панель итогов
        summary_layout = QHBoxLayout()
        
        self.revenue_with_vat_label = QLabel("Выручка с НДС: 0 ₽")
        self.expenses_with_vat_label = QLabel("Затраты с НДС: 0 ₽")
        self.gross_profit_with_vat_label = QLabel("Валовая прибыль: 0 ₽")
        self.profit_without_vat_label = QLabel("Прибыль без НДС: 0 ₽")
        self.vat_to_budget_net_label = QLabel("НДС в бюджет: 0 ₽")
        self.profit_tax_label = QLabel("Налог на прибыль: 0 ₽")

        for label in [self.revenue_with_vat_label, self.expenses_with_vat_label,
                    self.gross_profit_with_vat_label, self.profit_without_vat_label,
                    self.vat_to_budget_net_label, self.profit_tax_label]:
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

        summary_layout.addWidget(self.revenue_with_vat_label)
        summary_layout.addWidget(self.expenses_with_vat_label)
        summary_layout.addWidget(self.gross_profit_with_vat_label)
        summary_layout.addWidget(self.profit_without_vat_label)
        summary_layout.addWidget(self.vat_to_budget_net_label)
        summary_layout.addWidget(self.profit_tax_label)
        summary_layout.addStretch()

        table_layout.addLayout(summary_layout)

        # Вкладка с графиками
        self.charts_tab = QWidget()
        charts_layout = QVBoxLayout(self.charts_tab)

        # Область с прокруткой
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

        # Контейнер для графиков
        charts_container = QWidget()
        charts_container_layout = QVBoxLayout(charts_container)
        charts_container_layout.setSpacing(40)  # Увеличили с 20 до 40 пикселей
        charts_container_layout.setContentsMargins(10, 10, 10, 10)

        # Первая фигура
        self.figure, self.axes = plt.subplots(2, 2, figsize=(12, 10))
        self.figure.patch.set_facecolor('#f5f5f5')
        self.figure.subplots_adjust(hspace=0.4, wspace=0.3)  # Добавляем отступы между подграфиками внутри фигуры
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setMinimumHeight(550)  # Увеличили высоту
        charts_container_layout.addWidget(self.canvas)

        # Добавляем разделитель (необязательно)
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setFrameShadow(QFrame.Shadow.Sunken)
        charts_container_layout.addWidget(line)

        # Вторая фигура
        self.figure2, self.axes2 = plt.subplots(2, 2, figsize=(12, 10))
        self.figure2.patch.set_facecolor('#f5f5f5')
        self.figure2.subplots_adjust(hspace=0.4, wspace=0.3)  # Добавляем отступы между подграфиками
        self.canvas2 = FigureCanvas(self.figure2)
        self.canvas2.setMinimumHeight(550)  # Увеличили высоту
        charts_container_layout.addWidget(self.canvas2)

        # Добавляем разделитель
        line2 = QFrame()
        line2.setFrameShape(QFrame.Shape.HLine)
        line2.setFrameShadow(QFrame.Shadow.Sunken)
        charts_container_layout.addWidget(line2)

        # Третья фигура
        self.figure3, self.axes3 = plt.subplots(1, 1, figsize=(12, 5))
        self.figure3.patch.set_facecolor('#f5f5f5')
        self.canvas3 = FigureCanvas(self.figure3)
        self.canvas3.setMinimumHeight(350)  # Увеличили высоту
        charts_container_layout.addWidget(self.canvas3)

        # Кнопка обновления
        charts_btn_layout = QHBoxLayout()
        self.update_charts_btn = QPushButton("Обновить графики")
        self.update_charts_btn.clicked.connect(self.update_charts)
        self.update_charts_btn.setStyleSheet(self.apply_filter_btn.styleSheet())
        charts_container_layout.addLayout(charts_btn_layout)

        charts_container_layout.addStretch()


        scroll_area.setWidget(charts_container)
        charts_layout.addWidget(scroll_area)

        self.tab_widget.addTab(self.table_tab, "📊 Таблица данных")
        self.tab_widget.addTab(self.charts_tab, "📈 Графики и анализ")

        right_layout.addWidget(self.tab_widget)
        self.splitter.addWidget(right_panel)
        self.splitter.setSizes([250, self.width() - 250])

        # !!! ВАЖНО - ДОБАВЛЯЕМ СПЛИТТЕР В MAIN_LAYOUT !!!
        main_layout.addWidget(self.splitter)

        # Инициализация данными
        self.current_df = pd.DataFrame()
        self.display_data(self.current_df)
        self.update_summary()
        self.update_charts()
        self.update_filter_combos()

    #================================================================================
    # Создание меню
    def create_menus(self):
        menubar = self.menuBar()

        # КНОПКА ОБРАБОТАТЬ - ПЕРВАЯ В СТРОКЕ МЕНЮ
        self.process_selected_btn = QPushButton("ОБРАБОТАТЬ")
        self.process_selected_btn.setEnabled(False)
        self.process_selected_btn.setStyleSheet("""
            QPushButton {
                background-color: #ff4444;
                color: white;
                font-weight: bold;
                font-size: 14px;
                padding: 5px 15px;
                border-radius: 4px;
                margin: 2px 5px;
            }
            QPushButton:hover {
                background-color: #ff6666;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
        """)
        self.process_selected_btn.clicked.connect(self.process_selected_files)
        menubar.setCornerWidget(self.process_selected_btn, Qt.Corner.TopLeftCorner)

        # Меню "База данных"
        db_menu = menubar.addMenu("База данных")
        load_db_action = QAction("Загрузить БД", self)
        load_db_action.triggered.connect(self.load_database)
        db_menu.addAction(load_db_action)

        save_db_action = QAction("Сохранить БД", self)
        save_db_action.triggered.connect(self.save_database)
        db_menu.addAction(save_db_action)

        save_as_action = QAction("Сохранить БД как...", self)
        save_as_action.triggered.connect(self.save_database_as)
        db_menu.addAction(save_as_action)

        clear_db_action = QAction("Очистить БД", self)
        clear_db_action.triggered.connect(self.clear_database)
        db_menu.addAction(clear_db_action)

        export_db_action = QAction("Экспорт БД в Excel", self)
        export_db_action.triggered.connect(self.export_to_excel)
        db_menu.addAction(export_db_action)

        import_template_action = QAction("Импорт из шаблона", self)
        import_template_action.triggered.connect(self.import_from_template)
        db_menu.addAction(import_template_action)

        # Меню "Отчеты"
        report_menu = menubar.addMenu("Отчеты")
        quick_report_action = QAction("Быстрый отчет", self)
        quick_report_action.triggered.connect(self.generate_quick_report)
        report_menu.addAction(quick_report_action)

        report_menu.addSeparator()

        pdf_action = QAction("Экспорт в PDF", self)
        pdf_action.triggered.connect(self.export_to_pdf)
        report_menu.addAction(pdf_action)

        word_action = QAction("Экспорт в Word", self)
        word_action.triggered.connect(self.export_to_word)
        report_menu.addAction(word_action)

        # Меню "Настройки"
        settings_menu = menubar.addMenu("Настройки")
        settings_action = QAction("Настройки программы", self)
        settings_action.triggered.connect(self.show_settings)
        settings_menu.addAction(settings_action)

        # Меню "О программе"
        about_menu = menubar.addMenu("Помощь")
        about_action = QAction("О программе", self)
        about_action.triggered.connect(self.show_about)
        about_menu.addAction(about_action)
       
    #=======================================================
    #  Метод активации кнопки Обработать
    def _update_process_button_state(self):
        files = self.get_checked_files()
        self.process_selected_btn.setEnabled(len(files) > 0)


       # ==================== РАБОТА С ДЕРЕВОМ ФАЙЛОВ ====================
    def choose_root_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Выберите папку загрузки", self.load_folder)
        if folder:
            self.settings.setValue("input_folder", folder)
            self.load_folder = folder
            self.load_folder_tree(folder)

    def load_folder_tree(self, folder_path):
        self.tree_widget.clear()
        root_item = QTreeWidgetItem([os.path.basename(folder_path)])
        root_item.setData(0, Qt.ItemDataRole.UserRole, folder_path)
        root_item.setFlags(root_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        root_item.setCheckState(0, Qt.CheckState.Unchecked)
        self.tree_widget.addTopLevelItem(root_item)
        self._add_folder_contents(folder_path, root_item)
        root_item.setExpanded(True)

    def _add_folder_contents(self, path, parent_item):
        try:
            for item in sorted(os.listdir(path)):
                full_path = os.path.join(path, item)
                if os.path.isdir(full_path):
                    child = QTreeWidgetItem([item])
                    child.setData(0, Qt.ItemDataRole.UserRole, full_path)
                    child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                    child.setCheckState(0, Qt.CheckState.Unchecked)
                    parent_item.addChild(child)
                    self._add_folder_contents(full_path, child)
                elif item.lower().endswith(('.xlsx', '.xls')):
                    child = QTreeWidgetItem([item])
                    child.setData(0, Qt.ItemDataRole.UserRole, full_path)
                    child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                    child.setCheckState(0, Qt.CheckState.Unchecked)
                    parent_item.addChild(child)
        except Exception as e:
            print(f"Ошибка чтения папки {path}: {e}")

    def get_checked_files(self, item=None, files=None):
        if files is None:
            files = []
            root = self.tree_widget.topLevelItem(0)
            if root is None:
                return files
            self.get_checked_files(root, files)
            return files

        if item.checkState(0) == Qt.CheckState.Checked:
            file_path = item.data(0, Qt.ItemDataRole.UserRole)
            if file_path and os.path.isfile(file_path) and not os.path.basename(file_path).startswith('~$'):
                files.append(file_path)
        elif item.checkState(0) == Qt.CheckState.Checked and os.path.isdir(item.data(0, Qt.ItemDataRole.UserRole)):
            folder = item.data(0, Qt.ItemDataRole.UserRole)
            for root, dirs, files_in_folder in os.walk(folder):
                for f in files_in_folder:
                    if f.lower().endswith(('.xlsx', '.xls')) and not f.startswith('~$'):
                        files.append(os.path.join(root, f))
            return

        for i in range(item.childCount()):
            self.get_checked_files(item.child(i), files)

    def process_selected_files(self):
        files = self.get_checked_files()
        if not files:
            QMessageBox.information(self, "Ничего не выбрано", "Не выбрано ни одного файла для обработки.")
            return
        self.process_files(files)

    def _handle_item_changed(self, item, column):
        self.tree_widget.blockSignals(True)
        state = item.checkState(0)
        self._set_children_checkstate(item, state)
        self.tree_widget.blockSignals(False)

    def _set_children_checkstate(self, item, state):
        for i in range(item.childCount()):
            child = item.child(i)
            child.setCheckState(0, state)
            self._set_children_checkstate(child, state)

    # ==================== РАБОТА С БАЗОЙ ДАННЫХ ====================
    def clear_database(self):
        reply = QMessageBox.question(self, "Подтверждение",
                                      "Вы действительно хотите удалить все данные из базы?",
                                      QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            cursor = self.db.conn.cursor()
            cursor.execute("DELETE FROM reports")
            cursor.execute("DELETE FROM sqlite_sequence WHERE name='reports'")
            self.db.conn.commit()
            self.current_df = pd.DataFrame()
            self.display_data(self.current_df)
            self.update_summary()
            self.update_charts()
            self.update_filter_combos()
            QMessageBox.information(self, "Готово", "База данных очищена")

    def load_database(self):
        start_dir = self.db_load_folder if self.db_load_folder else ""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Выберите файл базы данных",
            start_dir,
            "SQLite DB (*.db)"
        )
        if not file_path:
            return

        try:
            self.db.conn.close()
            self.db = DatabaseManager(db_path=file_path)
            self.current_df = self.db.get_all_data()
            self.display_data(self.current_df)
            self.update_summary()
            self.update_charts()
            self.update_filter_combos()
            self.settings.setValue("last_database", file_path)
            QMessageBox.information(self, "Успех", f"База данных загружена из {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить базу данных:\n{str(e)}")

    def save_database(self):
        QMessageBox.information(self, "Сохранение", "Все изменения уже сохранены в текущей базе данных.")

    def save_database_as(self):
        cursor = self.db.conn.execute("PRAGMA database_list")
        row = cursor.fetchone()
        if row is None or not row[2]:
            QMessageBox.warning(self, "Предупреждение", "Не удалось определить путь к текущей базе данных")
            return
        current_db_path = row[2]

        start_dir = self.db_save_folder if self.db_save_folder else ""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Сохранить базу данных как",
            os.path.join(start_dir, "database.db"),
            "SQLite DB (*.db)"
        )
        if not file_path:
            return

        try:
            shutil.copy2(current_db_path, file_path)
            self.settings.setValue("last_database", file_path)
            QMessageBox.information(self, "Успех", f"База данных сохранена как {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить базу данных:\n{str(e)}")

    # ==================== ИМПОРТ ИЗ ШАБЛОНА ====================
    def import_from_template(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Выберите файл для импорта",
            self.load_folder,
            "Excel/CSV Files (*.xlsx *.xls *.csv)"
        )
        if not file_path:
            return

        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8-sig')
            else:
                df = pd.read_excel(file_path)

            if self._map_columns_and_import(df):
                QMessageBox.information(self, "Успех", "Данные импортированы")
                self.current_df = self.db.get_all_data()
                self.display_data(self.current_df)
                self.update_summary()
                self.update_charts()
                self.update_filter_combos()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось импортировать файл:\n{str(e)}")

    def _map_columns_and_import(self, df):
        cursor = self.db.conn.execute("PRAGMA table_info(reports)")
        db_columns = [col[1] for col in cursor.fetchall() if col[1] not in ['id', 'import_date']]

        dialog = QDialog(self)
        dialog.setWindowTitle("Сопоставление колонок")
        dialog.setMinimumWidth(600)
        layout = QVBoxLayout(dialog)

        layout.addWidget(QLabel("Сопоставьте колонки из файла с полями базы данных:"))

        mapping_table = QTableWidget(len(df.columns), 2)
        mapping_table.setHorizontalHeaderLabels(["Колонка в файле", "Поле в БД"])

        combo_boxes = []
        for i, col in enumerate(df.columns):
            mapping_table.setItem(i, 0, QTableWidgetItem(str(col)))
            combo = QComboBox()
            combo.addItems(db_columns)
            combo_boxes.append(combo)
            mapping_table.setCellWidget(i, 1, combo)

        layout.addWidget(mapping_table)

        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box.accepted.connect(dialog.accept)
        btn_box.rejected.connect(dialog.reject)
        layout.addWidget(btn_box)

        if dialog.exec() != QDialog.DialogCode.Accepted:
            return False

        mapping = {}
        for i, combo in enumerate(combo_boxes):
            target_col = combo.currentText()
            if target_col:
                mapping[df.columns[i]] = target_col

        if not mapping:
            QMessageBox.warning(self, "Предупреждение", "Не выбрано ни одного сопоставления")
            return False

        df_import = df.rename(columns=mapping)
        df_import = df_import[[col for col in mapping.values() if col in db_columns]]

        for col in db_columns:
            if col not in df_import.columns:
                df_import[col] = '' if 'date' in col or 'name' in col else 0

        self.db.save_data(df_import)
        return True

    # ==================== ОБРАБОТКА ФАЙЛОВ ====================
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
            except Exception as e:
                error_files.append(f"{os.path.basename(file_path)}: {str(e)}")

        progress.setValue(total)

        if success_count > 0:
            self.current_df = self.db.get_all_data()
            self.display_data(self.current_df)
            self.update_summary()
            self.update_charts()
            self.update_filter_combos()
            print(f"Загружено записей из БД: {len(self.current_df)}")

        msg = f"Успешно загружено: {success_count} из {total}"
        if error_files:
            msg += "\n\nОшибки:\n" + "\n".join(error_files[:5])
            if len(error_files) > 5:
                msg += f"\n... и ещё {len(error_files)-5} ошибок"
        QMessageBox.information(self, "Результат загрузки", msg)

    def _import_excel_file(self, file_path):
        print(f"Обработка файла: {os.path.basename(file_path)}")

        if os.path.basename(file_path).startswith('~$'):
            print("Пропуск временного файла")
            return 0

        if file_path.lower().endswith('.xls') and not file_path.lower().endswith('.xlsx'):
            try:
                import xlrd
            except ImportError:
                raise ImportError("Для чтения файлов .xls установите xlrd: pip install xlrd")

        try:
            df_preview = pd.read_excel(file_path, nrows=30, header=None, dtype=str)
        except Exception as e:
            print(f"Ошибка чтения с dtype=str: {e}")
            try:
                df_preview = pd.read_excel(file_path, nrows=30, header=None)
                df_preview = df_preview.astype(str)
            except Exception as e2:
                print(f"Не удалось прочитать файл {file_path}: {e2}")
                raise ValueError(f"Не удалось прочитать файл: {e2}")

        df_preview = df_preview.fillna('')
        preview_text = ' '.join(df_preview.values.flatten()).lower()
        preview_text = re.sub(r'\s+', ' ', preview_text)

        print(f"preview_text (первые 200): {preview_text[:200]}")

        if 'книга покупок' in preview_text:
            print("-> Распознана книга покупок")
            df = self._parse_purchase_book(file_path)
            return self.db.save_data(df)

        if 'книга продаж' in preview_text:
            print("-> Распознана книга продаж")
            df = self._parse_sales_book(file_path)
            return self.db.save_data(df)

        if 'оборотно-сальдовая ведомость по счету 19' in preview_text or 'анализ счета 19' in preview_text:
            print("-> Распознан ОСВ 19")
            df = self._parse_osv_19_detailed(file_path)
            return self.db.save_data(df)

        if 'оборотно-сальдовая ведомость по счету 41' in preview_text:
            print("-> Распознан ОСВ 41")
            df = self._parse_osv_41_detailed(file_path)
            return self.db.save_data(df)

        if 'оборотно-сальдовая ведомость по счету 44' in preview_text:
            print("-> Распознан ОСВ 44")
            df = self._parse_osv_44_detailed(file_path)
            return self.db.save_data(df)

        if 'оборотно-сальдовая ведомость по счету 60' in preview_text:
            print("-> Распознан ОСВ 60")
            df = self._parse_osv_60_detailed(file_path)
            return self.db.save_data(df)

        if 'оборотно-сальдовая ведомость по счету 62' in preview_text:
            print("-> Распознан ОСВ 62")
            df = self._parse_osv_62_detailed(file_path)
            return self.db.save_data(df)

        if 'оборотно-сальдовая ведомость по счету 68' in preview_text:
            print("-> Распознан ОСВ 68")
            df = self._parse_osv_68_detailed(file_path)
            return self.db.save_data(df)

        if 'оборотно-сальдовая ведомость по счету 90' in preview_text:
            print("-> Распознан ОСВ 90")
            df = self._parse_osv_90_detailed(file_path)
            return self.db.save_data(df)

        if 'оборотно-сальдовая ведомость по счету 91' in preview_text:
            print("-> Распознан ОСВ 91")
            df = self._parse_osv_91_detailed(file_path)
            return self.db.save_data(df)

        if 'отчет по продажам' in preview_text:
            print("-> Распознан отчет по продажам")
            return self._parse_sales_report_detailed(file_path)

        print("-> Не распознан тип, пробуем legacy импорт")
        return self._import_legacy(file_path)

    # ==================== ПАРСЕРЫ ====================
    def _extract_company_by_keyword(self, df, keyword):
        for i in range(min(15, len(df))):
            row = df.iloc[i].tolist()
            for j, cell in enumerate(row):
                if keyword.lower() in cell.lower():
                    for k in range(j+1, len(row)):
                        if row[k].strip():
                            return row[k].strip()
                    break
        return "Неизвестная компания"

    def _extract_base_number(self, cell):
        import re
        if not isinstance(cell, str):
            cell = str(cell)
        match = re.match(r'^(\d+)', cell.strip())
        return int(match.group(1)) if match else None

    def _find_header_row_loose(self, df, min_required=5):
        for i in range(len(df)):
            row = df.iloc[i].tolist()
            expected = 1
            indices = {}
            for col_idx, cell in enumerate(row):
                base = self._extract_base_number(cell)
                if base is not None:
                    if base == expected:
                        indices[base] = col_idx
                        expected += 1
            if expected - 1 >= min_required:
                for col_idx, cell in enumerate(row):
                    base = self._extract_base_number(cell)
                    if base is not None and base not in indices:
                        indices[base] = col_idx
                return i, indices
        return None, None

    def _find_header_row_fallback(self, df, min_count=5):
        best_row = None
        best_indices = {}
        max_count = 0
        for i in range(len(df)):
            row = df.iloc[i].tolist()
            indices = {}
            for col_idx, cell in enumerate(row):
                base = self._extract_base_number(cell)
                if base is not None and base not in indices:
                    indices[base] = col_idx
            if len(indices) >= min_count and len(indices) > max_count:
                max_count = len(indices)
                best_row = i
                best_indices = indices
        if best_row is not None:
            return best_row, best_indices
        return None, None

    def _clean_number(self, value):
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, bytes):
            try:
                s = value.decode('utf-8')
            except:
                s = str(value)
        else:
            s = str(value)
        s = s.strip().replace(' ', '').replace(',', '.').replace('−', '-').replace('—', '-')
        import re
        s = re.sub(r'[^\d.-]', '', s)
        try:
            return float(s) if s else 0.0
        except:
            return 0.0

    def _month_name_to_number(self, month_name):
        month_names = {
            'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04', 'май': '05', 'июн': '06',
            'июл': '07', 'авг': '08', 'сен': '09', 'окт': '10', 'ноя': '11', 'дек': '12'
        }
        for key, num in month_names.items():
            if key in month_name.lower():
                return num
        return '01'

    # ========== КНИГА ПОКУПОК ==========
    def _parse_purchase_book(self, file_path):
        import pandas as pd
        import re
        from datetime import datetime

        df = pd.read_excel(file_path, header=None, dtype=str)
        df = df.fillna('').astype(str).apply(lambda col: col.str.strip())

        company = "Неизвестная компания"
        for i in range(min(10, len(df))):
            row = df.iloc[i].tolist()
            for j, cell in enumerate(row):
                if 'покупатель' in cell.lower():
                    for k in range(j+1, len(row)):
                        if row[k].strip():
                            company = row[k].strip()
                            break
                    if company != "Неизвестная компания":
                        break
            if company != "Неизвестная компания":
                break
        print(f"Книга покупок: компания = {company}")

        header_text = ' '.join(df.iloc[:20].values.flatten()).lower()
        period_match = re.search(r'с\s+(\d{2}\.\d{2}\.\d{4})\s+по\s+(\d{2}\.\d{2}\.\d{4})', header_text, re.IGNORECASE)
        if not period_match:
            raise ValueError("Не найден период в книге покупок")
        period_start = datetime.strptime(period_match.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
        period_end = datetime.strptime(period_match.group(2), "%d.%m.%Y").strftime("%Y-%m-%d")

        header_row_idx, num_to_idx = self._find_header_row_loose(df, min_required=5)
        if header_row_idx is None:
            header_row_idx, num_to_idx = self._find_header_row_fallback(df, min_count=8)
            if header_row_idx is None:
                raise ValueError("Не найдена строка с номерами колонок")

        print(f"Книга покупок: строка с номерами на индексе {header_row_idx}")
        print(f"Соответствие базовых номеров колонкам: {num_to_idx}")

        required_nums = [2, 3, 8, 9, 14, 15]
        for num in required_nums:
            if num not in num_to_idx:
                raise ValueError(f"Не найден номер колонки {num}")

        op_col = num_to_idx[2]
        doc_col = num_to_idx[3]
        accept_col = num_to_idx[8]
        seller_col = num_to_idx[9]
        amount_col = num_to_idx[14]
        vat_col = num_to_idx[15]

        records = []
        current_seller = None
        data_start = header_row_idx + 1

        for i in range(data_start, len(df)):
            row = df.iloc[i].tolist()
            first_cell_raw = row[0] if len(row) > 0 else ''
            first_cell = str(first_cell_raw).strip().lower() if first_cell_raw else ''

            if not first_cell:
                continue

            if 'всего по продавцу' in first_cell:
                current_seller = None
                continue

            if first_cell == 'всего' and 'по продавцу' not in first_cell:
                break

            if first_cell.replace('.', '', 1).replace(',', '').isdigit():
                seller = current_seller
                if seller_col < len(row) and row[seller_col] and row[seller_col].strip():
                    seller = row[seller_col].strip()
                    current_seller = seller
                elif not seller:
                    continue

                doc_str = row[doc_col] if doc_col < len(row) else ''
                doc_number = ''
                doc_date = ''
                if doc_str:
                    parts = re.split(r'\s+от\s+', doc_str, maxsplit=1, flags=re.IGNORECASE)
                    if len(parts) == 2:
                        doc_number = parts[0].strip()
                        doc_date = parts[1].strip()
                    else:
                        doc_number = doc_str

                operation_code = row[op_col] if op_col < len(row) else ''
                acceptance_date = row[accept_col] if accept_col < len(row) else ''

                amount = self._clean_number(row[amount_col] if amount_col < len(row) else '0')
                vat = self._clean_number(row[vat_col] if vat_col < len(row) else '0')

                if amount == 0 and vat == 0:
                    continue

                records.append({
                    'company': company,
                    'period_start': period_start,
                    'period_end': period_end,
                    'doc_type': 'purchase_book',
                    'product_group': 'Покупки',
                    'seller': seller,
                    'buyer': '',
                    'document_number': doc_number,
                    'document_date': doc_date,
                    'operation_code': operation_code,
                    'acceptance_date': acceptance_date,
                    'purchase_amount_with_vat': amount,
                    'sales_amount_with_vat': 0.0,
                    'sales_amount_without_vat': 0.0,
                    'vat_deductible': vat,
                    'vat_to_budget': 0.0,
                    'nomenclature': '',
                    'revenue': 0.0,
                    'cost_price': 0.0,
                    'gross_profit': 0.0,
                    'sales_expenses': 0.0,
                    'other_income_expenses': 0.0,
                    'net_profit': 0.0,
                    'payment_document': '',
                    'quantity': 1
                })
            else:
                if not first_cell[0].isdigit():
                    current_seller = row[0].strip()

        if not records:
            raise ValueError("Не найдено записей в книге покупок")

        print(f"Книга покупок: найдено записей — {len(records)}")
        return pd.DataFrame(records)

    # ========== КНИГА ПРОДАЖ ==========
    def _parse_sales_book(self, file_path):
        import pandas as pd
        import re
        from datetime import datetime

        print(f"Парсер продаж: начало обработки {file_path}")
        df = pd.read_excel(file_path, header=None, dtype=str)
        df = df.fillna('').astype(str).apply(lambda col: col.str.strip())
        print(f"Прочитано строк: {len(df)}")

        company = "Неизвестная компания"
        for i in range(min(10, len(df))):
            row = df.iloc[i].tolist()
            for j, cell in enumerate(row):
                if 'продавец' in cell.lower():
                    for k in range(j+1, len(row)):
                        if row[k].strip():
                            company = row[k].strip()
                            break
                    if company != "Неизвестная компания":
                        break
            if company != "Неизвестная компания":
                break
        print(f"Книга продаж: компания = {company}")

        header_text = ' '.join(df.iloc[:20].values.flatten()).lower()
        period_match = re.search(r'с\s+(\d{2}\.\d{2}\.\d{4})\s+по\s+(\d{2}\.\d{2}\.\d{4})', header_text, re.IGNORECASE)
        if not period_match:
            raise ValueError("Не найден период в книге продаж")
        period_start = datetime.strptime(period_match.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
        period_end = datetime.strptime(period_match.group(2), "%d.%m.%Y").strftime("%Y-%m-%d")
        print(f"Период: {period_start} - {period_end}")

        header_row_idx, num_to_idx = self._find_header_row_loose(df, min_required=5)
        if header_row_idx is None:
            header_row_idx, num_to_idx = self._find_header_row_fallback(df, min_count=8)
            if header_row_idx is None:
                for debug_i in range(min(20, len(df))):
                    print(f"Строка {debug_i}: {df.iloc[debug_i].tolist()}")
                raise ValueError("Не найдена строка с номерами колонок")

        print(f"Книга продаж: строка с номерами на индексе {header_row_idx}")
        print(f"Соответствие базовых номеров колонкам: {num_to_idx}")

        required_nums = [2, 3, 7, 8, 11, 13, 14, 17]
        for num in required_nums:
            if num not in num_to_idx:
                raise ValueError(f"Не найден базовый номер колонки {num}")

        header_row = df.iloc[header_row_idx].tolist()

        op_col = num_to_idx[2]
        doc_col = num_to_idx[3]
        buyer_col = num_to_idx[7]
        inn_col = num_to_idx[8]
        payment_col = num_to_idx[11]
        amount_without_vat_col = num_to_idx[14]
        vat_col = num_to_idx[17]

        base13_start = num_to_idx[13]
        amount_with_vat_col = None
        for offset in range(10):
            if base13_start + offset >= len(header_row):
                break
            cell = header_row[base13_start + offset]
            clean = re.sub(r'\s+', '', cell.lower())
            if '13б' in clean:
                amount_with_vat_col = base13_start + offset
                break
        if amount_with_vat_col is None:
            raise ValueError("Не найдена колонка '13б' (сумма с НДС)")

        print(f"Индексы Excel: операция={op_col}, документ={doc_col}, покупатель={buyer_col}, ИНН={inn_col}, оплата={payment_col}, сумма без НДС={amount_without_vat_col}, НДС={vat_col}, сумма с НДС={amount_with_vat_col}")

        records = []
        current_buyer = None
        data_start = header_row_idx + 1

        for i in range(data_start, len(df)):
            row = df.iloc[i].tolist()
            first_cell_raw = row[0] if len(row) > 0 else ''
            first_cell = str(first_cell_raw).strip().lower() if first_cell_raw else ''

            if not first_cell:
                continue

            if 'всего по покупателю' in first_cell:
                current_buyer = None
                continue

            if first_cell == 'всего' and 'по покупателю' not in first_cell:
                print(f"Достигнута финальная строка 'Всего' на строке {i}")
                break

            if first_cell.replace('.', '', 1).replace(',', '').isdigit():
                buyer = current_buyer
                if buyer_col < len(row) and row[buyer_col] and row[buyer_col].strip():
                    buyer = row[buyer_col].strip()
                    current_buyer = buyer
                elif not buyer:
                    continue

                doc_str = row[doc_col] if doc_col < len(row) else ''
                doc_number = ''
                doc_date = ''
                if doc_str:
                    parts = re.split(r'\s+от\s+', doc_str, maxsplit=1, flags=re.IGNORECASE)
                    if len(parts) == 2:
                        doc_number = parts[0].strip()
                        doc_date = parts[1].strip()
                    else:
                        doc_number = doc_str

                operation_code = row[op_col] if op_col < len(row) else ''
                inn = row[inn_col] if inn_col < len(row) else ''
                payment_doc = row[payment_col] if payment_col < len(row) else ''

                amount_with_vat = self._clean_number(row[amount_with_vat_col] if amount_with_vat_col < len(row) else '0')
                amount_without_vat = self._clean_number(row[amount_without_vat_col] if amount_without_vat_col < len(row) else '0')
                vat = self._clean_number(row[vat_col] if vat_col < len(row) else '0')

                if amount_with_vat == 0 and amount_without_vat == 0 and vat == 0:
                    continue

                records.append({
                    'company': company,
                    'period_start': period_start,
                    'period_end': period_end,
                    'doc_type': 'sales_book',
                    'product_group': 'Продажи',
                    'seller': '',
                    'buyer': buyer,
                    'document_number': doc_number,
                    'document_date': doc_date,
                    'operation_code': operation_code,
                    'payment_document': payment_doc,
                    'sales_amount_with_vat': amount_with_vat,
                    'sales_amount_without_vat': amount_without_vat,
                    'vat_to_budget': vat,
                    'vat_deductible': 0.0,
                    'nomenclature': '',
                    'revenue': amount_without_vat,
                    'cost_price': 0.0,
                    'gross_profit': 0.0,
                    'sales_expenses': 0.0,
                    'other_income_expenses': 0.0,
                    'net_profit': 0.0,
                    'purchase_amount_with_vat': 0.0,
                    'acceptance_date': '',
                    'quantity': 1
                })
            else:
                if not first_cell[0].isdigit():
                    current_buyer = row[0].strip()

        if not records:
            raise ValueError("Не найдено записей в книге продаж")

        print(f"Книга продаж: найдено записей — {len(records)}")
        return pd.DataFrame(records)

    # ========== ОСВ (заглушки) ==========
    def _parse_osv_19_detailed(self, file_path):
        print("ОСВ 19: найдено записей — 0")
        return pd.DataFrame()

    def _parse_osv_41_detailed(self, file_path):
        print("ОСВ 41: найдено записей — 0")
        return pd.DataFrame()

    def _parse_osv_44_detailed(self, file_path):
        print("ОСВ 44: найдено записей — 0")
        return pd.DataFrame()

    def _parse_osv_60_detailed(self, file_path):
        print("ОСВ 60: найдено записей — 0")
        return pd.DataFrame()

    def _parse_osv_62_detailed(self, file_path):
        print("ОСВ 62: найдено записей — 0")
        return pd.DataFrame()

    def _parse_osv_68_detailed(self, file_path):
        print("ОСВ 68: найдено записей — 0")
        return pd.DataFrame()

    def _parse_osv_90_detailed(self, file_path):
        print("ОСВ 90: найдено записей — 0")
        return pd.DataFrame()

    def _parse_osv_91_detailed(self, file_path):
        print("ОСВ 91: найдено записей — 0")
        return pd.DataFrame()

    def _parse_sales_report_detailed(self, file_path):
        print("Отчет по продажам: заглушка")
        return 0

    def _import_legacy(self, file_path):
        print("Legacy импорт: заглушка")
        return 0

    # ==================== ОТОБРАЖЕНИЕ ДАННЫХ ====================
    def display_data(self, df):
        self.table_model.setRowCount(0)

        column_order = [
            'id', 'company', 'period_start', 'period_end', 'doc_type', 'product_group',
            'seller', 'buyer', 'nomenclature',
            'document_number', 'document_date', 'operation_code', 'acceptance_date', 'payment_document',
            'purchase_amount_with_vat', 'sales_amount_without_vat', 'sales_amount_with_vat',
            'revenue', 'cost_price', 'gross_profit',
            'sales_expenses', 'other_income_expenses', 'net_profit',
            'vat_deductible', 'vat_to_budget', 'quantity', 'import_date'
        ]

        ru_headers = {
            'id': 'ID',
            'company': 'Компания',
            'period_start': 'Период с',
            'period_end': 'Период по',
            'doc_type': 'Тип',
            'product_group': 'Группа',
            'seller': 'Продавец',
            'buyer': 'Покупатель',
            'nomenclature': 'Номенклатура',
            'document_number': '№ сч/ф',
            'document_date': 'Дата сч/ф',
            'operation_code': 'Код опер.',
            'acceptance_date': 'Дата принятия',
            'payment_document': 'Плат. док.',
            'purchase_amount_with_vat': 'Сумма покупки с НДС',
            'sales_amount_without_vat': 'Сумма продажи без НДС',
            'sales_amount_with_vat': 'Сумма продажи с НДС',
            'revenue': 'Выручка',
            'cost_price': 'Себестоимость',
            'gross_profit': 'Валовая прибыль',
            'sales_expenses': 'Расходы на продажу',
            'other_income_expenses': 'Прочие доходы/расходы',
            'net_profit': 'Чистая прибыль',
            'vat_deductible': 'НДС покупки',
            'vat_to_budget': 'НДС продажи',
            'quantity': 'Кол-во',
            'import_date': 'Дата импорта'
        }

        headers = [ru_headers.get(col, col) for col in column_order]
        self.table_model.setHorizontalHeaderLabels(headers)

        if df is None or df.empty:
            return

        for _, row in df.iterrows():
            items = []
            for col in column_order:
                value = row[col] if col in row.index else ''
                if col in ['purchase_amount_with_vat', 'sales_amount_without_vat', 'sales_amount_with_vat',
                           'revenue', 'cost_price', 'gross_profit', 'sales_expenses',
                           'other_income_expenses', 'net_profit', 'vat_deductible', 'vat_to_budget']:
                    if isinstance(value, (int, float)):
                        display_value = f"{value:,.2f} ₽".replace(",", " ")
                    else:
                        display_value = str(value)
                elif col == 'quantity':
                    if isinstance(value, (int, float)):
                        display_value = str(int(value))
                    else:
                        display_value = str(value)
                else:
                    display_value = str(value)
                item = QStandardItem(display_value)
                item.setData(value)
                items.append(item)
            self.table_model.appendRow(items)

        self.table_view.resizeColumnsToContents()

    # ==================== ФИЛЬТРЫ ====================
    def update_filter_combos(self):
        current_company = self.company_combo.currentText()
        current_period = self.period_combo.currentText()
        current_group = self.group_combo.currentText()

        self.company_combo.clear()
        self.period_combo.clear()
        self.group_combo.clear()

        self.company_combo.addItem("Все компании")
        self.period_combo.addItem("Все периоды")
        self.group_combo.addItem("Все группы")

        if self.current_df is not None and not self.current_df.empty:
            companies = sorted(self.current_df['company'].dropna().unique())
            self.company_combo.addItems([str(c) for c in companies])

            periods = set()
            for date_str in self.current_df['period_start'].dropna().unique():
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    periods.add(dt.strftime("%m.%Y"))
                except:
                    pass
            self.period_combo.addItems(sorted(periods))

            groups = sorted(self.current_df['product_group'].dropna().unique())
            self.group_combo.addItems([str(g) for g in groups])

        index = self.company_combo.findText(current_company)
        if index >= 0:
            self.company_combo.setCurrentIndex(index)
        index = self.period_combo.findText(current_period)
        if index >= 0:
            self.period_combo.setCurrentIndex(index)
        index = self.group_combo.findText(current_group)
        if index >= 0:
            self.group_combo.setCurrentIndex(index)

    def _period_to_dates(self, period_str):
        import calendar
        try:
            month, year = period_str.split('.')
            month = int(month)
            year = int(year)
            start_date = f"{year:04d}-{month:02d}-01"
            last_day = calendar.monthrange(year, month)[1]
            end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
            return start_date, end_date
        except:
            return None, None

    def apply_filters(self):
        company = self.company_combo.currentText()
        period = self.period_combo.currentText()
        product_group = self.group_combo.currentText()

        date_from = None
        date_to = None
        if period != "Все периоды":
            date_from, date_to = self._period_to_dates(period)

        filtered_df = self.db.get_filtered_data(
            company=company if company != "Все компании" else None,
            date_from=date_from,
            date_to=date_to,
            product_group=product_group if product_group != "Все группы" else None
        )

        if not filtered_df.empty:
            self.current_df = filtered_df
            self.display_data(filtered_df)
            self.update_summary()
            self.update_charts()
        else:
            self.current_df = pd.DataFrame()
            self.display_data(self.current_df)
            self.update_summary()
            self.update_charts()

    # ==================== РАСЧЁТ ФИНАНСОВЫХ ПОКАЗАТЕЛЕЙ ====================
    def calculate_financials(self, df=None):
        if df is None:
            df = self.current_df
        if df is None or df.empty:
            return {
                'revenue_with_vat': 0.0,
                'revenue_without_vat': 0.0,
                'expenses_with_vat': 0.0,
                'expenses_without_vat': 0.0,
                'gross_profit_with_vat': 0.0,
                'profit_without_vat': 0.0,
                'profit_margin': 0.0,
                'vat_sales': 0.0,
                'vat_purchases': 0.0,
                'vat_to_budget_net': 0.0,
                'profit_tax': 0.0,
            }

        sales_df = df[df['doc_type'] == 'sales_book']
        revenue_with_vat = sales_df['sales_amount_with_vat'].sum() if 'sales_amount_with_vat' in sales_df else 0.0
        revenue_without_vat = sales_df['sales_amount_without_vat'].sum() if 'sales_amount_without_vat' in sales_df else 0.0
        vat_sales = sales_df['vat_to_budget'].sum() if 'vat_to_budget' in sales_df else 0.0

        purchases_df = df[df['doc_type'] == 'purchase_book']
        expenses_with_vat = purchases_df['purchase_amount_with_vat'].sum() if 'purchase_amount_with_vat' in purchases_df else 0.0
        vat_purchases = purchases_df['vat_deductible'].sum() if 'vat_deductible' in purchases_df else 0.0
        expenses_without_vat = expenses_with_vat - vat_purchases

        gross_profit_with_vat = revenue_with_vat - expenses_with_vat
        profit_without_vat = revenue_without_vat - expenses_without_vat

        profit_margin = (profit_without_vat / revenue_without_vat * 100) if revenue_without_vat != 0 else 0.0

        vat_to_budget_net = vat_sales - vat_purchases
        profit_tax = profit_without_vat * 0.25  # 25% налог на прибыль

        return {
            'revenue_with_vat': revenue_with_vat,
            'revenue_without_vat': revenue_without_vat,
            'expenses_with_vat': expenses_with_vat,
            'expenses_without_vat': expenses_without_vat,
            'gross_profit_with_vat': gross_profit_with_vat,
            'profit_without_vat': profit_without_vat,
            'profit_margin': profit_margin,
            'vat_sales': vat_sales,
            'vat_purchases': vat_purchases,
            'vat_to_budget_net': vat_to_budget_net,
            'profit_tax': profit_tax,
        }

    def update_summary(self):
        fin = self.calculate_financials()
        self.revenue_with_vat_label.setText(f"Выручка с НДС: {fin['revenue_with_vat']:,.0f} ₽".replace(",", " "))
        self.expenses_with_vat_label.setText(f"Затраты с НДС: {fin['expenses_with_vat']:,.0f} ₽".replace(",", " "))
        self.gross_profit_with_vat_label.setText(f"Валовая прибыль: {fin['gross_profit_with_vat']:,.0f} ₽".replace(",", " "))
        self.profit_without_vat_label.setText(f"Прибыль без НДС: {fin['profit_without_vat']:,.0f} ₽".replace(",", " "))
        self.vat_to_budget_net_label.setText(f"НДС в бюджет: {fin['vat_to_budget_net']:,.0f} ₽".replace(",", " "))
        self.profit_tax_label.setText(f"Налог на прибыль: {fin['profit_tax']:,.0f} ₽".replace(",", " "))

    #===========================================================================================
    # ==================== ГРАФИКИ ====================
    def update_charts(self):
        if self.current_df is None or self.current_df.empty:
            for fig in [self.figure, self.figure2, self.figure3]:
                for ax in fig.axes:
                    ax.clear()
                    ax.text(0.5, 0.5, 'Нет данных для отображения', 
                        ha='center', va='center', fontsize=12)
            self.canvas.draw()
            self.canvas2.draw()
            self.canvas3.draw()
            return

        df_clean = self.current_df.fillna(0)
        
        # Определяем год для заголовка
        year_text = "Графики за "
        if 'period_start' in df_clean.columns and not df_clean['period_start'].empty:
            years = sorted(pd.to_datetime(df_clean['period_start']).dt.year.unique())
            if len(years) == 1:
                year_text += f"{years[0]} год"
            else:
                year_text += f"{min(years)}-{max(years)} годы"
        else:
            year_text += "весь период"
        
        # Добавляем кварталы
        if 'period_start' in df_clean.columns:
            df_clean['quarter'] = pd.to_datetime(df_clean['period_start']).dt.to_period('Q')
            df_clean['quarter_str'] = df_clean['quarter'].astype(str).str.replace('Q', ' Кв.')

        sales_df = df_clean[df_clean['doc_type'] == 'sales_book']
        purchases_df = df_clean[df_clean['doc_type'] == 'purchase_book']

        # Очищаем все фигуры
        for fig in [self.figure, self.figure2, self.figure3]:
            for ax in fig.axes:
                ax.clear()

        # Настройки шрифтов для всех графиков
        plt.rcParams.update({
            'font.size': 9,
            'axes.titlesize': 11,
            'axes.labelsize': 9,
            'xtick.labelsize': 8,
            'ytick.labelsize': 8,
            'legend.fontsize': 8
        })

        # Функция для добавления значений над столбцами
        def add_values(bars, ax, format_str='{:.0f}'):
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                        format_str.format(height).replace(",", " "),
                        ha='center', va='bottom', fontsize=7)

        # ========== ФИГУРА 1: Основные графики ==========
        self.figure.suptitle(year_text, fontsize=14, fontweight='bold')
        
        # 1. Распределение прибыли по товарным группам
        try:
            if 'product_group' in df_clean.columns:
                group_profit = df_clean.groupby('product_group')['net_profit'].sum()
                if not group_profit.empty and group_profit.sum() != 0:
                    colors1 = plt.cm.Set3(np.linspace(0, 1, len(group_profit)))
                    wedges, texts, autotexts = self.axes[0, 0].pie(
                        group_profit.values, 
                        labels=group_profit.index,
                        autopct='%1.1f%%', 
                        colors=colors1, 
                        startangle=90,
                        textprops={'fontsize': 8}
                    )
                    self.axes[0, 0].set_title('1. Распределение прибыли', fontsize=10)
                else:
                    self.axes[0, 0].text(0.5, 0.5, 'Нет данных', ha='center', va='center')
        except Exception as e:
            self.axes[0, 0].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 2. ТОП-5 товаров
        try:
            if not sales_df.empty and 'nomenclature' in sales_df.columns:
                product_profit = sales_df.groupby('nomenclature')['net_profit'].sum().reset_index()
                product_profit = product_profit[product_profit['nomenclature'] != '']
                if not product_profit.empty:
                    top_products = product_profit.nlargest(5, 'net_profit')
                    labels = [str(x)[:15] + '...' if len(str(x)) > 15 else str(x)
                            for x in top_products['nomenclature']]
                    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(top_products)))
                    bars = self.axes[0, 1].barh(labels, top_products['net_profit'], color=colors)
                    self.axes[0, 1].set_title('2. ТОП-5 товаров по прибыльности', fontsize=10)
                    self.axes[0, 1].set_xlabel('Прибыль, ₽', fontsize=8)
                    # Добавляем значения
                    for bar in bars:
                        width = bar.get_width()
                        if width > 0:
                            self.axes[0, 1].text(width, bar.get_y() + bar.get_height()/2,
                                                f'{width:,.0f}'.replace(",", " "),
                                                ha='left', va='center', fontsize=7)
        except Exception as e:
            self.axes[0, 1].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 3. Закупки с НДС по кварталам
        try:
            if not purchases_df.empty and 'quarter' in purchases_df.columns:
                purchases_q = purchases_df.groupby('quarter')['purchase_amount_with_vat'].sum().reset_index()
                purchases_q['quarter_str'] = purchases_q['quarter'].astype(str).str.replace('Q', '')
                colors = plt.cm.Oranges(np.linspace(0.3, 0.8, len(purchases_q)))
                x_pos = range(len(purchases_q))
                bars = self.axes[1, 0].bar(x_pos, purchases_q['purchase_amount_with_vat'], color=colors)
                self.axes[1, 0].set_title('3. Закупки с НДС', fontsize=10)
                self.axes[1, 0].set_ylabel('Сумма, ₽', fontsize=8)
                self.axes[1, 0].set_xticks(x_pos)
                self.axes[1, 0].set_xticklabels(purchases_q['quarter_str'], fontsize=7)
                add_values(bars, self.axes[1, 0])
        except Exception as e:
            self.axes[1, 0].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 4. Выручка с НДС по кварталам
        try:
            if not sales_df.empty and 'quarter' in sales_df.columns:
                revenue_q = sales_df.groupby('quarter')['sales_amount_with_vat'].sum().reset_index()
                revenue_q['quarter_str'] = revenue_q['quarter'].astype(str).str.replace('Q', '')
                colors = plt.cm.Blues(np.linspace(0.3, 0.8, len(revenue_q)))
                x_pos = range(len(revenue_q))
                bars = self.axes[1, 1].bar(x_pos, revenue_q['sales_amount_with_vat'], color=colors)
                self.axes[1, 1].set_title('4. Выручка с НДС', fontsize=10)
                self.axes[1, 1].set_ylabel('Сумма, ₽', fontsize=8)
                self.axes[1, 1].set_xticks(x_pos)
                self.axes[1, 1].set_xticklabels(revenue_q['quarter_str'], fontsize=7)
                add_values(bars, self.axes[1, 1])
        except Exception as e:
            self.axes[1, 1].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        self.figure.subplots_adjust(hspace=0.4, wspace=0.3)
        self.canvas.draw()

        # ========== ФИГУРА 2: Следующие 4 графика ==========
        self.figure2.suptitle(year_text, fontsize=14, fontweight='bold')
        
        # 5. НДС в бюджет по кварталам
        try:
            if 'quarter' in df_clean.columns:
                vat_budget = df_clean.groupby('quarter').apply(
                    lambda x: x['vat_to_budget'].sum() - x['vat_deductible'].sum()
                ).reset_index(name='vat_budget')
                vat_budget['quarter_str'] = vat_budget['quarter'].astype(str).str.replace('Q', '')
                colors = plt.cm.Reds(np.linspace(0.3, 0.8, len(vat_budget)))
                x_pos = range(len(vat_budget))
                bars = self.axes2[0, 0].bar(x_pos, vat_budget['vat_budget'], color=colors)
                self.axes2[0, 0].set_title('5. НДС в бюджет', fontsize=10)
                self.axes2[0, 0].set_ylabel('Сумма НДС, ₽', fontsize=8)
                self.axes2[0, 0].set_xticks(x_pos)
                self.axes2[0, 0].set_xticklabels(vat_budget['quarter_str'], fontsize=7)
                add_values(bars, self.axes2[0, 0])
        except Exception as e:
            self.axes2[0, 0].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 6. НДС по выручке
        try:
            if not sales_df.empty and 'quarter' in sales_df.columns:
                vat_sales_q = sales_df.groupby('quarter')['vat_to_budget'].sum().reset_index()
                vat_sales_q['quarter_str'] = vat_sales_q['quarter'].astype(str).str.replace('Q', '')
                colors = plt.cm.Greens(np.linspace(0.3, 0.8, len(vat_sales_q)))
                x_pos = range(len(vat_sales_q))
                bars = self.axes2[0, 1].bar(x_pos, vat_sales_q['vat_to_budget'], color=colors)
                self.axes2[0, 1].set_title('6. НДС по выручке', fontsize=10)
                self.axes2[0, 1].set_ylabel('Сумма НДС, ₽', fontsize=8)
                self.axes2[0, 1].set_xticks(x_pos)
                self.axes2[0, 1].set_xticklabels(vat_sales_q['quarter_str'], fontsize=7)
                add_values(bars, self.axes2[0, 1])
        except Exception as e:
            self.axes2[0, 1].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 7. НДС по затратам
        try:
            if not purchases_df.empty and 'quarter' in purchases_df.columns:
                vat_purchases_q = purchases_df.groupby('quarter')['vat_deductible'].sum().reset_index()
                vat_purchases_q['quarter_str'] = vat_purchases_q['quarter'].astype(str).str.replace('Q', '')
                colors = plt.cm.Oranges(np.linspace(0.3, 0.8, len(vat_purchases_q)))
                x_pos = range(len(vat_purchases_q))
                bars = self.axes2[1, 0].bar(x_pos, vat_purchases_q['vat_deductible'], color=colors)
                self.axes2[1, 0].set_title('7. НДС по затратам', fontsize=10)
                self.axes2[1, 0].set_ylabel('Сумма НДС, ₽', fontsize=8)
                self.axes2[1, 0].set_xticks(x_pos)
                self.axes2[1, 0].set_xticklabels(vat_purchases_q['quarter_str'], fontsize=7)
                add_values(bars, self.axes2[1, 0])
        except Exception as e:
            self.axes2[1, 0].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        # 8. Валовая прибыль
        try:
            if not sales_df.empty and not purchases_df.empty:
                revenue_q = sales_df.groupby('quarter')['sales_amount_with_vat'].sum().reset_index()
                expenses_q = purchases_df.groupby('quarter')['purchase_amount_with_vat'].sum().reset_index()
                profit_q = pd.merge(revenue_q, expenses_q, on='quarter', how='outer').fillna(0)
                profit_q['gross_profit'] = profit_q['sales_amount_with_vat'] - profit_q['purchase_amount_with_vat']
                profit_q['quarter_str'] = profit_q['quarter'].astype(str).str.replace('Q', '')
                colors = plt.cm.Purples(np.linspace(0.3, 0.8, len(profit_q)))
                x_pos = range(len(profit_q))
                bars = self.axes2[1, 1].bar(x_pos, profit_q['gross_profit'], color=colors)
                self.axes2[1, 1].set_title('8. Валовая прибыль', fontsize=10)
                self.axes2[1, 1].set_ylabel('Прибыль, ₽', fontsize=8)
                self.axes2[1, 1].set_xticks(x_pos)
                self.axes2[1, 1].set_xticklabels(profit_q['quarter_str'], fontsize=7)
                add_values(bars, self.axes2[1, 1])
        except Exception as e:
            self.axes2[1, 1].text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        self.figure2.subplots_adjust(hspace=0.4, wspace=0.3)
        self.canvas2.draw()

        # ========== ФИГУРА 3: 9-й график ==========
        self.figure3.suptitle(f"{year_text} - Затраты", fontsize=14, fontweight='bold')
        
        try:
            if not purchases_df.empty and 'quarter' in purchases_df.columns:
                expenses_q = purchases_df.groupby('quarter')['purchase_amount_with_vat'].sum().reset_index()
                expenses_q['quarter_str'] = expenses_q['quarter'].astype(str).str.replace('Q', '')
                colors = plt.cm.Reds(np.linspace(0.3, 0.8, len(expenses_q)))
                x_pos = range(len(expenses_q))
                bars = self.axes3.bar(x_pos, expenses_q['purchase_amount_with_vat'], color=colors)
                self.axes3.set_title('9. Затраты по кварталам (все налоги и закупки)', fontsize=11)
                self.axes3.set_ylabel('Сумма затрат, ₽', fontsize=9)
                self.axes3.set_xticks(x_pos)
                self.axes3.set_xticklabels(expenses_q['quarter_str'], fontsize=8)
                self.axes3.grid(True, alpha=0.3, axis='y')
                add_values(bars, self.axes3)
        except Exception as e:
            self.axes3.text(0.5, 0.5, 'Ошибка', ha='center', va='center')

        self.figure3.subplots_adjust(bottom=0.15)
        self.canvas3.draw()
        
        # Сохраняем для экспорта
        self.chart_path2 = "temp_chart2.png"
        self.chart_path3 = "temp_chart3.png"
        self.figure2.savefig(self.chart_path2, format='png', dpi=150, bbox_inches='tight')
        self.figure3.savefig(self.chart_path3, format='png', dpi=150, bbox_inches='tight')


    #=====================================================================
    # ==================== ЭКСПОРТ В EXCEL ====================
    def export_to_excel(self):
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Предупреждение", "Нет данных для экспорта")
            return

        ru_headers = {
            'id': 'ID',
            'company': 'Компания',
            'period_start': 'Период с',
            'period_end': 'Период по',
            'doc_type': 'Тип',
            'product_group': 'Группа',
            'seller': 'Продавец',
            'buyer': 'Покупатель',
            'nomenclature': 'Номенклатура',
            'document_number': '№ сч/ф',
            'document_date': 'Дата сч/ф',
            'operation_code': 'Код опер.',
            'acceptance_date': 'Дата принятия',
            'payment_document': 'Плат. док.',
            'purchase_amount_with_vat': 'Сумма покупки с НДС',
            'sales_amount_without_vat': 'Сумма продажи без НДС',
            'sales_amount_with_vat': 'Сумма продажи с НДС',
            'revenue': 'Выручка',
            'cost_price': 'Себестоимость',
            'gross_profit': 'Валовая прибыль',
            'sales_expenses': 'Расходы на продажу',
            'other_income_expenses': 'Прочие доходы/расходы',
            'net_profit': 'Чистая прибыль',
            'vat_deductible': 'НДС покупки',
            'vat_to_budget': 'НДС продажи',
            'quantity': 'Кол-во',
            'import_date': 'Дата импорта'
        }

        df_export = self.current_df.copy()
        df_export.rename(columns=ru_headers, inplace=True)

        file_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить как Excel", 
            os.path.join(self.save_folder, "отчет_buh_tuund.xlsx") if self.save_folder else "отчет_buh_tuund.xlsx",
            "Excel Files (*.xlsx)"
        )
        if not file_path:
            return

        try:
            buf = io.BytesIO()
            self.figure.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            buf.seek(0)

            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df_export.to_excel(writer, sheet_name='Данные', index=False)

                summary_df = pd.DataFrame({
                    'Показатель': ['Общая выручка', 'НДС продажи', 'НДС покупки', 'НДС в бюджет',
                                   'Валовая прибыль', 'Прибыль без НДС', 'Налог на прибыль',
                                   'Количество записей', 'Дата экспорта'],
                    'Значение': [
                        f"{self.current_df['revenue'].sum():,.0f} ₽".replace(",", " "),
                        f"{self.current_df['vat_to_budget'].sum():,.0f} ₽".replace(",", " "),
                        f"{self.current_df['vat_deductible'].sum():,.0f} ₽".replace(",", " "),
                        f"{self.current_df['vat_to_budget'].sum() - self.current_df['vat_deductible'].sum():,.0f} ₽".replace(",", " "),
                        f"{self.current_df['gross_profit'].sum():,.0f} ₽".replace(",", " "),
                        f"{self.current_df['net_profit'].sum():,.0f} ₽".replace(",", " "),
                        f"{self.current_df['net_profit'].sum() * 0.25:,.0f} ₽".replace(",", " "),
                        len(self.current_df),
                        datetime.now().strftime("%d.%m.%Y %H:%M")
                    ]
                })
                summary_df.to_excel(writer, sheet_name='Итоги', index=False)

                workbook = writer.book
                for sheet_name in workbook.sheetnames:
                    worksheet = workbook[sheet_name]
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
                    for cell in worksheet[1]:
                        cell.font = Font(bold=True)

            QMessageBox.information(self, "Успех", f"Файл сохранен: {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при экспорте: {str(e)}")

    #========================================================================================
    # ==================== ЭКСПОРТ В PDF ====================
    def export_to_pdf(self):
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Предупреждение", "Нет данных для экспорта")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить как PDF",
            os.path.join(self.save_folder, "отчет_buh_tuund.pdf") if self.save_folder else "отчет_buh_tuund.pdf",
            "PDF Files (*.pdf)"
        )
        if not file_path:
            return

        try:
            try:
                pdfmetrics.registerFont(TTFont('Arial', 'arial.ttf'))
                font_available = True
            except:
                font_available = False

            doc = SimpleDocTemplate(file_path, pagesize=A4)
            elements = []
            styles = getSampleStyleSheet()

            # Добавляем логотип
            if os.path.exists("logo.png"):
                logo = Image("logo.png", width=50, height=50)
                logo.hAlign = TA_CENTER
                elements.append(logo)
                elements.append(Spacer(1, 10))

            if font_available:
                for style_name in styles.byName:
                    styles[style_name].fontName = 'Arial'

            company_name = "Неизвестная компания"
            if not self.current_df.empty and 'company' in self.current_df.columns:
                unique_companies = self.current_df['company'].dropna().unique()
                if len(unique_companies) > 0:
                    company_name = unique_companies[0]

            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontName='Arial' if font_available else styles['Heading1'].fontName,
                fontSize=16,
                alignment=TA_CENTER,
                spaceAfter=20,
                textColor=colors.HexColor('#2c3e50')
            )
            elements.append(Paragraph(f"БУХГАЛТЕРСКИЙ ОТЧЕТ<br/>{company_name}", title_style))

            period_str = "не определен"
            if not self.current_df.empty and 'period_start' in self.current_df.columns and 'period_end' in self.current_df.columns:
                try:
                    start_min = self.current_df['period_start'].min()
                    end_max = self.current_df['period_end'].max()
                    start_dt = datetime.strptime(start_min, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_max, "%Y-%m-%d")
                    period_str = f"с {start_dt.strftime('%d.%m.%Y')} по {end_dt.strftime('%d.%m.%Y')}"
                except:
                    period_str = f"с {start_min} по {end_max}"

            info_text = f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M')} | Записей: {len(self.current_df)}"
            elements.append(Paragraph(info_text, styles['Normal']))
            elements.append(Paragraph(f"Отчетный период: {period_str}", styles['Normal']))
            elements.append(Spacer(1, 20))

            fin = self.calculate_financials()
            elements.append(Paragraph("<b>ФИНАНСОВЫЕ ПОКАЗАТЕЛИ</b>", styles['Heading2']))
            elements.append(Paragraph(f"Выручка с НДС: {fin['revenue_with_vat']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"Выручка без НДС: {fin['revenue_without_vat']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"Затраты с НДС: {fin['expenses_with_vat']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"Затраты без НДС: {fin['expenses_without_vat']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"Валовая прибыль (с НДС): {fin['gross_profit_with_vat']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"Прибыль без НДС: {fin['profit_without_vat']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"Норма прибыли: {fin['profit_margin']:.2f}%", styles['Normal']))
            elements.append(Paragraph(f"НДС продажи: {fin['vat_sales']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"НДС покупки: {fin['vat_purchases']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"НДС в бюджет: {fin['vat_to_budget_net']:,.0f} ₽", styles['Normal']))
            elements.append(Paragraph(f"Налог на прибыль (25%): {fin['profit_tax']:,.0f} ₽", styles['Normal']))
            elements.append(Spacer(1, 20))

            # ===========================================
            chart_path = "temp_chart.png"
            self.figure.savefig(chart_path, format='png', dpi=150, bbox_inches='tight')
            elements.append(Paragraph("Визуализация данных:", styles['Heading2']))
            elements.append(Image(chart_path, width=400, height=300))
            elements.append(Spacer(1, 20))
           
            # ===========================================
            # После основного графика
            # После основного графика (self.figure)
            elements.append(Paragraph("Дополнительные графики (часть 1):", styles['Heading2']))
            elements.append(Image(self.chart_path2, width=500, height=400))
            elements.append(Spacer(1, 20))

            elements.append(Paragraph("Дополнительные графики (часть 2):", styles['Heading2']))
            elements.append(Image(self.chart_path3, width=500, height=300))
            elements.append(Spacer(1, 20))



            elements.append(Paragraph("Данные отчета (первые 20 записей):", styles['Heading2']))
            table_data = [['Период', 'Компания', 'Контрагент', 'Выручка с НДС', 'НДС продажи', 'Прибыль']]
            for _, row in self.current_df.head(20).iterrows():
                counterparty = row.get('buyer', '') or row.get('seller', '') or row.get('nomenclature', '')
                profit = row.get('net_profit', 0)
                period_str = row.get('period_start', '')
                if period_str and isinstance(period_str, str):
                    try:
                        dt = datetime.strptime(period_str, "%Y-%m-%d")
                        period_str = dt.strftime("%m.%Y")
                    except:
                        pass
                table_data.append([
                    period_str,
                    str(row.get('company', ''))[:20],
                    counterparty[:20],
                    f"{row.get('sales_amount_with_vat', 0):,.0f} ₽".replace(",", " "),
                    f"{row.get('vat_to_budget', 0):,.0f} ₽".replace(",", " "),
                    f"{profit:,.0f} ₽".replace(",", " ")
                ])

            table = Table(table_data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Arial' if font_available else 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('FONTNAME', (0, 1), (-1, -1), 'Arial' if font_available else 'Helvetica'),
            ]))
            elements.append(table)
            elements.append(Spacer(1, 20))

            footer_style = ParagraphStyle(
                'Footer',
                parent=styles['Italic'],
                fontName='Arial' if font_available else styles['Italic'].fontName,
                fontSize=8,
                alignment=TA_CENTER,
                textColor=colors.grey
            )
            elements.append(Paragraph("Сформировано программой BuhTuundOtchet", footer_style))

            doc.build(elements)

            if os.path.exists(chart_path):
                os.remove(chart_path)
            # В самом конце метода, перед QMessageBox.information
            # Удаление временных файлов графиков
            if hasattr(self, 'chart_path2') and os.path.exists(self.chart_path2):
                os.remove(self.chart_path2)
            if hasattr(self, 'chart_path3') and os.path.exists(self.chart_path3):
                os.remove(self.chart_path3)
            if os.path.exists("temp_chart.png"):
                os.remove("temp_chart.png")
                
        
            QMessageBox.information(self, "Успех", f"PDF файл сохранен: {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при экспорте в PDF: {str(e)}")

    #====================================================================
    # ==================== ЭКСПОРТ В WORD ====================
    def export_to_word(self):
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Предупреждение", "Нет данных для экспорта")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить как Word",
            os.path.join(self.save_folder, "отчет_buh_tuund.docx") if self.save_folder else "отчет_buh_tuund.docx",
            "Word Files (*.docx)"
        )
        if not file_path:
            return

        try:
            doc = docx.Document()
            
            # Добавляем логотип
            if os.path.exists("logo.png"):
                doc.add_picture("logo.png", width=Inches(1))
                # Центрируем логотип
                last_paragraph = doc.paragraphs[-1]
                last_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER

            company_name = "Неизвестная компания"
            if not self.current_df.empty and 'company' in self.current_df.columns:
                unique_companies = self.current_df['company'].dropna().unique()
                if len(unique_companies) > 0:
                    company_name = unique_companies[0]

            title = doc.add_heading(f'БУХГАЛТЕРСКИЙ ОТЧЕТ {company_name}', 0)
            title.alignment = WD_ALIGN_PARAGRAPH.CENTER

            doc.add_paragraph(f'Дата формирования: {datetime.now().strftime("%d.%m.%Y %H:%M")}')

            period_str = "не определен"
            if not self.current_df.empty and 'period_start' in self.current_df.columns and 'period_end' in self.current_df.columns:
                try:
                    start_min = self.current_df['period_start'].min()
                    end_max = self.current_df['period_end'].max()
                    start_dt = datetime.strptime(start_min, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_max, "%Y-%m-%d")
                    period_str = f"с {start_dt.strftime('%d.%m.%Y')} по {end_dt.strftime('%d.%m.%Y')}"
                except:
                    period_str = f"с {start_min} по {end_max}"
            doc.add_paragraph(f"Отчетный период: {period_str}")
            doc.add_paragraph()

            fin = self.calculate_financials()
            doc.add_heading('ФИНАНСОВЫЕ ПОКАЗАТЕЛИ', level=2)
            doc.add_paragraph(f"Выручка с НДС: {fin['revenue_with_vat']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"Выручка без НДС: {fin['revenue_without_vat']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"Затраты с НДС: {fin['expenses_with_vat']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"Затраты без НДС: {fin['expenses_without_vat']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"Валовая прибыль (с НДС): {fin['gross_profit_with_vat']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"Прибыль без НДС: {fin['profit_without_vat']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"Норма прибыли: {fin['profit_margin']:.2f}%")
            doc.add_paragraph(f"НДС продажи: {fin['vat_sales']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"НДС покупки: {fin['vat_purchases']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"НДС в бюджет: {fin['vat_to_budget_net']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph(f"Налог на прибыль (25%): {fin['profit_tax']:,.0f} ₽".replace(",", " "))
            doc.add_paragraph()

            chart_path = "temp_chart_word.png"
            self.figure.savefig(chart_path, format='png', dpi=150, bbox_inches='tight')
            doc.add_heading('Визуализация данных:', level=2)
            doc.add_picture(chart_path, width=Inches(6))
            doc.add_paragraph()

            # После основного графика
            doc.add_heading('Дополнительные графики (часть 1):', level=2)
            doc.add_picture(self.chart_path2, width=Inches(6))
            doc.add_paragraph()

            doc.add_heading('Дополнительные графики (часть 2):', level=2)
            doc.add_picture(self.chart_path3, width=Inches(6))
            doc.add_paragraph()


            doc.add_heading('Данные отчета (первые 15 записей):', level=2)

            table = doc.add_table(rows=1, cols=6)
            table.style = 'LightShading-Accent1'
            hdr_cells = table.rows[0].cells
            hdr_cells[0].text = 'Период'
            hdr_cells[1].text = 'Компания'
            hdr_cells[2].text = 'Контрагент'
            hdr_cells[3].text = 'Выручка с НДС'
            hdr_cells[4].text = 'НДС продажи'
            hdr_cells[5].text = 'Прибыль без НДС'

            for cell in hdr_cells:
                for paragraph in cell.paragraphs:
                    for run in paragraph.runs:
                        run.font.bold = True

            for _, row in self.current_df.head(15).iterrows():
                cells = table.add_row().cells
                period_str = row.get('period_start', '')
                if period_str and isinstance(period_str, str):
                    try:
                        dt = datetime.strptime(period_str, "%Y-%m-%d")
                        period_str = dt.strftime("%m.%Y")
                    except:
                        pass
                cells[0].text = str(period_str)
                cells[1].text = str(row.get('company', ''))[:30]
                counterparty = row.get('buyer', '') or row.get('seller', '') or row.get('nomenclature', '')
                cells[2].text = counterparty[:30]
                cells[3].text = f"{row.get('sales_amount_with_vat', 0):,.0f} ₽".replace(",", " ")
                cells[4].text = f"{row.get('vat_to_budget', 0):,.0f} ₽".replace(",", " ")
                cells[5].text = f"{row.get('net_profit', 0):,.0f} ₽".replace(",", " ")

            doc.add_paragraph()

            footer = doc.add_paragraph('Сформировано программой BuhTuundOtchet v7.0.1')
            footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
            footer.italic = True

            doc.save(file_path)

            if os.path.exists(chart_path):
                os.remove(chart_path)


            # В самом конце метода, перед QMessageBox.information
            # Удаление временных файлов графиков
            if hasattr(self, 'chart_path2') and os.path.exists(self.chart_path2):
                os.remove(self.chart_path2)
            if hasattr(self, 'chart_path3') and os.path.exists(self.chart_path3):
                os.remove(self.chart_path3)
            if os.path.exists("temp_chart_word.png"):
                os.remove("temp_chart_word.png")

            QMessageBox.information(self, "Успех", f"Word файл сохранен: {file_path}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при экспорте в Word: {str(e)}")

    # ==================== БЫСТРЫЙ ОТЧЕТ ====================
    def generate_quick_report(self):
        if self.current_df is None or self.current_df.empty:
            QMessageBox.warning(self, "Предупреждение", "Нет данных для отчета")
            return

        fin = self.calculate_financials()

        company_name = "Неизвестная компания"
        if not self.current_df.empty and 'company' in self.current_df.columns:
            unique_companies = self.current_df['company'].dropna().unique()
            if len(unique_companies) > 0:
                company_name = unique_companies[0]

        period_str = "не определен"
        if not self.current_df.empty and 'period_start' in self.current_df.columns and 'period_end' in self.current_df.columns:
            try:
                start_min = self.current_df['period_start'].min()
                end_max = self.current_df['period_end'].max()
                start_dt = datetime.strptime(start_min, "%Y-%m-%d")
                end_dt = datetime.strptime(end_max, "%Y-%m-%d")
                period_str = f"с {start_dt.strftime('%d.%m.%Y')} по {end_dt.strftime('%d.%m.%Y')}"
            except:
                period_str = f"с {start_min} по {end_max}"

        report_plain = f"""БЫСТРЫЙ ОТЧЕТ BUHTUUNDOTCHET
        Компания: {company_name}
        Период анализа: {period_str}
        Товарная группа: {self.group_combo.currentText()}

        ОСНОВНЫЕ ПОКАЗАТЕЛИ:
        - Выручка с НДС: {fin['revenue_with_vat']:,.0f} ₽
        - Выручка без НДС: {fin['revenue_without_vat']:,.0f} ₽
        - Затраты с НДС: {fin['expenses_with_vat']:,.0f} ₽
        - Затраты без НДС: {fin['expenses_without_vat']:,.0f} ₽
        - Валовая прибыль (с НДС): {fin['gross_profit_with_vat']:,.0f} ₽
        - Прибыль без НДС: {fin['profit_without_vat']:,.0f} ₽
        - Норма прибыли: {fin['profit_margin']:.2f}%
        - НДС продажи: {fin['vat_sales']:,.0f} ₽
        - НДС покупки: {fin['vat_purchases']:,.0f} ₽
        - НДС в бюджет: {fin['vat_to_budget_net']:,.0f} ₽
        - Налог на прибыль: {fin['profit_tax']:,.0f} ₽

        ТОП-5 товаров по прибыльности:
        {self._get_top_products_text()}

        Сформировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}"""
        
        # Добавляем логотип в HTML (если хотите)
        logo_html = ""
        if os.path.exists("logo.png"):
            # Кодируем изображение в base64 для вставки в HTML
            import base64
            with open("logo.png", "rb") as f:
                logo_data = base64.b64encode(f.read()).decode()
            logo_html = f'<img src="data:image/png;base64,{logo_data}" width="100" height="100" style="float:right;">'
        
        report_html = f"""
        {logo_html}
        <h3>БЫСТРЫЙ ОТЧЕТ BUHTUUNDOTCHET</h3>
        <p><b>Компания:</b> {company_name}</p>
        <p><b>Период анализа:</b> {period_str}</p>
        <p><b>Товарная группа:</b> {self.group_combo.currentText()}</p>
        <hr>
        <p><b>ОСНОВНЫЕ ПОКАЗАТЕЛИ:</b></p>
        <p>• Выручка с НДС: <span style='color: #27ae60; font-weight: bold;'>{fin['revenue_with_vat']:,.0f} ₽</span></p>
        <p>• Выручка без НДС: <span style='color: #27ae60; font-weight: bold;'>{fin['revenue_without_vat']:,.0f} ₽</span></p>
        <p>• Затраты с НДС: <span style='color: #e74c3c; font-weight: bold;'>{fin['expenses_with_vat']:,.0f} ₽</span></p>
        <p>• Затраты без НДС: <span style='color: #e74c3c; font-weight: bold;'>{fin['expenses_without_vat']:,.0f} ₽</span></p>
        <p>• Валовая прибыль (с НДС): <span style='color: #3498db; font-weight: bold;'>{fin['gross_profit_with_vat']:,.0f} ₽</span></p>
        <p>• Прибыль без НДС: <span style='color: #3498db; font-weight: bold;'>{fin['profit_without_vat']:,.0f} ₽</span></p>
        <p>• Норма прибыли: <span style='color: #f39c12; font-weight: bold;'>{fin['profit_margin']:.2f}%</span></p>
        <p>• НДС продажи: <span style='color: #9b59b6; font-weight: bold;'>{fin['vat_sales']:,.0f} ₽</span></p>
        <p>• НДС покупки: <span style='color: #9b59b6; font-weight: bold;'>{fin['vat_purchases']:,.0f} ₽</span></p>
        <p>• НДС в бюджет: <span style='color: #e67e22; font-weight: bold;'>{fin['vat_to_budget_net']:,.0f} ₽</span></p>
        <p>• Налог на прибыль: <span style='color: #e67e22; font-weight: bold;'>{fin['profit_tax']:,.0f} ₽</span></p>
        <hr>
        <p><b>ТОП-5 товаров по прибыльности:</b></p>
        <pre>{self._get_top_products_text()}</pre>
        <hr>
        <p><i>Сформировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}</i></p>
        """

        dlg = QDialog(self)
        dlg.setWindowTitle("Быстрый отчет")
        dlg.setMinimumSize(600, 500)
        layout = QVBoxLayout(dlg)

        text_edit = QTextEdit()
        text_edit.setHtml(report_html)
        text_edit.setReadOnly(True)
        layout.addWidget(text_edit)

        button_box = QDialogButtonBox()
        btn_copy = QPushButton("Копировать")
        btn_copy.clicked.connect(lambda: self._copy_report_to_clipboard(report_plain))
        btn_save = QPushButton("Сохранить как...")
        btn_save.clicked.connect(lambda: self._save_report_to_txt(report_plain))
        btn_close = QPushButton("Закрыть")
        btn_close.clicked.connect(dlg.accept)

        button_box.addButton(btn_copy, QDialogButtonBox.ButtonRole.ActionRole)
        button_box.addButton(btn_save, QDialogButtonBox.ButtonRole.ActionRole)
        button_box.addButton(btn_close, QDialogButtonBox.ButtonRole.RejectRole)

        layout.addWidget(button_box)
        dlg.exec()

    def _copy_report_to_clipboard(self, text):
        clipboard = QApplication.clipboard()
        clipboard.setText(text)
        QMessageBox.information(self, "Готово", "Отчет скопирован в буфер обмена")

    def _save_report_to_txt(self, text):
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить отчет как...",
            os.path.join(self.save_folder, "отчет.txt") if self.save_folder else "отчет.txt",
            "Text Files (*.txt)"
        )
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(text)
                QMessageBox.information(self, "Успех", f"Отчет сохранен в {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить файл: {str(e)}")

    def _get_top_products_text(self):
        profit_col = 'net_profit'
        if profit_col in self.current_df.columns and not self.current_df.empty:
            top_products = self.current_df.nlargest(5, profit_col)[['nomenclature', profit_col]]
            lines = []
            for _, row in top_products.iterrows():
                name = row['nomenclature'] if row['nomenclature'] else "Без названия"
                lines.append(f"{name}: {row[profit_col]:,.0f} ₽".replace(",", " "))
            return "\n".join(lines)
        return "Нет данных"

    # ==================== О ПРОГРАММЕ ====================
    def show_about(self):
        # Создаём диалог
        about_dialog = QDialog(self)
        about_dialog.setWindowTitle("О программе BuhTuundOtchet")
        about_dialog.setMinimumWidth(500)
        layout = QVBoxLayout(about_dialog)
        
        # Добавляем логотип
        if os.path.exists("logo.png"):
            logo_label = QLabel()
            pixmap = QPixmap("logo.png")
            # Масштабируем до разумного размера (например, 100x100)
            scaled_pixmap = pixmap.scaled(100, 100, Qt.AspectRatioMode.KeepAspectRatio, 
                                        Qt.TransformationMode.SmoothTransformation)
            logo_label.setPixmap(scaled_pixmap)
            logo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(logo_label)
        
        # Текст о программе
        about_text = """<h2>Программа BuhTuundOtchet</h2>
        <p><b>Версия программы:</b> v7.1.0</p>
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
            <li>Расчет НДС продажи, покупки, в бюджет</li>
            <li>Расчет налога на прибыль (25%)</li>
            <li>Визуализация данных (графики и диаграммы)</li>
            <li>Экспорт в Excel, PDF, Word</li>
            <li>Современный интерфейс с меню и настройками</li>
        </ul>
        <p><b>Используемые технологии:</b> Python, PyQt6, Pandas, Matplotlib, SQLite, ReportLab, python-docx</p>
        """
        
        text_label = QLabel(about_text)
        text_label.setTextFormat(Qt.TextFormat.RichText)
        text_label.setWordWrap(True)
        layout.addWidget(text_label)
        
        # Кнопка закрытия
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        button_box.accepted.connect(about_dialog.accept)
        layout.addWidget(button_box)
        
        about_dialog.exec()


# ==================== ЗАПУСК ПРОГРАММЫ ====================
def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    # Устанавливаем иконку приложения
    if os.path.exists("logo.png"):
        app.setWindowIcon(QIcon("logo.png"))
    else:
        app.setWindowIcon(QIcon.fromTheme("office-chart-line"))
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()