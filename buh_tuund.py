#=====================================================
# Buh_Tuund v5.11.0

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
from PyQt6.QtWidgets import QSplitter, QTreeWidget, QTreeWidgetItem, QAbstractItemView, QPushButton, QFileDialog 
from PyQt6.QtCore import Qt
from PyQt6.QtCore import QSettings

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
                company TEXT,
                period_start TEXT,
                period_end TEXT,
                doc_type TEXT,
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
            'purchase_amount_with_vat': 'REAL',
            'sales_amount_without_vat': 'REAL'
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
        query = "SELECT * FROM reports ORDER BY period_start DESC, company, doc_type"
        return pd.read_sql_query(query, self.conn)

    def get_filtered_data(
            self,
            company=None,
            date_from=None,
            date_to=None,
            product_group=None,
            doc_type=None
        ):
        """
        Универсальный фильтр данных.

        Параметры:
        - company: название компании
        - date_from: 'YYYY-MM-DD'
        - date_to: 'YYYY-MM-DD'
        - product_group: Покупки / Продажи / ОСВ
        - doc_type: purchase / sales / osv_19 и т.д.
        """

        query = "SELECT * FROM reports WHERE 1=1"
        params = []

        # Фильтр по компании
        if company and company != "Все компании":
            query += " AND company = ?"
            params.append(company)

        # Фильтр по диапазону дат
        if date_from:
            query += " AND period_start >= ?"
            params.append(date_from)

        if date_to:
            query += " AND period_end <= ?"
            params.append(date_to)

        # Фильтр по группе
        if product_group and product_group != "Все группы":
            query += " AND product_group = ?"
            params.append(product_group)

        # Фильтр по типу документа
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
        self.init_ui()
        self.settings = QSettings("Компания", "BuhTuund")
        self._load_saved_paths()
    
    def init_ui(self):
        self.setWindowTitle("BuhTuundOtchet")
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

        
        # ===================================================================
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

        # ==============================  боковая панель =====================================
        # Создаём QSplitter для разделения на панели
        # Создаём главный сплиттер
        self.splitter = QSplitter(Qt.Orientation.Horizontal)

        # --- Левая панель с деревом ---
        self.left_panel = QWidget()
        left_layout = QVBoxLayout(self.left_panel)
        left_layout.setContentsMargins(2, 2, 2, 2)

        # Кнопка выбора корневой папки (над деревом)
        self.select_root_btn = QPushButton("Выбрать папку...")
        self.select_root_btn.clicked.connect(self.choose_root_folder)
        left_layout.addWidget(self.select_root_btn)

        # Дерево с чекбоксами
        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderHidden(True)
        self.tree_widget.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left_layout.addWidget(self.tree_widget)

        # Кнопка "Обработать" под деревом
        self.process_selected_btn = QPushButton("Обработать выбранное")
        self.process_selected_btn.clicked.connect(self.process_selected_files)
        left_layout.addWidget(self.process_selected_btn)
        self.tree_widget.itemChanged.connect(self._handle_item_changed)


        #----------------ПРАВАЯ ПАНЕЛЬ --------------------------
        # --- Правая панель ---------(ваш существующий центральный виджет) ---
        # Предполагается, что у вас уже есть central_widget со всем содержимым
        # Если нет, создайте его аналогично вашему коду
        # В вашем коде central_widget, вероятно, уже создан и назначен через setCentralWidget.
        # Чтобы не нарушить структуру, мы извлечём его из self.centralWidget() после того, как он будет создан.
        # Убедитесь, что перед этим вы уже создали и назначили центральный виджет.
        right_panel = self.centralWidget()  # должен быть создан ранее

        # Добавляем панели в сплиттер
        self.splitter.addWidget(self.left_panel)
        self.splitter.addWidget(right_panel)
        self.splitter.setSizes([250, self.width() - 250])

        # Устанавливаем сплиттер как новый центральный виджет
        self.setCentralWidget(self.splitter)

        #======================================================================================
        
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


    def _finalize_and_save(self, data_rows):
        """
        Универсальная обработка перед сохранением:
        - нормализация типов
        - автоматический пересчёт прибыли
        - защита от NaN
        """

        if not data_rows:
            return 0

        df = pd.DataFrame(data_rows)

        # Обязательные колонки (если отсутствуют — создаём)
        required_columns = [
            'company', 'period', 'counterparty', 'document_number',
            'operation_type', 'quantity',
            'revenue', 'vat_in_revenue', 'cost_price',
            'gross_profit', 'sales_expenses',
            'other_income_expenses', 'net_profit',
            'vat_deductible', 'vat_to_budget'
        ]

        for col in required_columns:
            if col not in df.columns:
                df[col] = 0 if col != 'counterparty' and col != 'document_number' and col != 'operation_type' else ""

        # Количество
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)

        # Числовые поля
        numeric_cols = [
            'revenue','vat_in_revenue','cost_price',
            'sales_expenses','other_income_expenses',
            'vat_deductible','vat_to_budget'
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 🔥 Автоматический пересчёт прибыли
        df['gross_profit'] = df['revenue'] - df['vat_in_revenue'] - df['cost_price']

        df['net_profit'] = (
            df['gross_profit']
            - df['sales_expenses']
            + df['other_income_expenses']
        )

        return self.db.save_data(df)
    
    def _safe_float(self, value):
        try:
            if isinstance(value, str):
                value = value.replace(" ", "").replace(",", ".")
            return float(value)
        except:
            return 0.0

    # ==================================================================================
    # Сохранение и загрузка настроек
    def choose_root_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Выберите папку загрузки")
        if folder:
            self.settings.setValue("input_folder", folder)
            self.load_folder_tree(folder)
    def _load_saved_paths(self):
        input_path = self.settings.value("input_folder", "")
        output_path = self.settings.value("output_folder", "")

        if input_path and os.path.exists(input_path):
            self.load_folder_tree(input_path)

        if output_path and os.path.exists(output_path):
            self.output_folder = output_path
    def choose_output_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Выберите папку выгрузки")
        if folder:
            self.settings.setValue("output_folder", folder)
            self.output_folder = folder
    # ==================================================================================
    # Методы для работы с деревом
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
                elif item.lower().endswith('.xlsx'):
                    child = QTreeWidgetItem([item])
                    child.setData(0, Qt.ItemDataRole.UserRole, full_path)
                    child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                    child.setCheckState(0, Qt.CheckState.Unchecked)
                    parent_item.addChild(child)
        except Exception as e:
            print(f"Ошибка чтения папки {path}: {e}")

    def get_checked_files(self, item=None, files=None):
        """Рекурсивно собирает пути всех отмеченных файлов."""
        if files is None:
            files = []
            root = self.tree_widget.topLevelItem(0)
            if root is None:
                return files
            self.get_checked_files(root, files)
            return files

        # Если элемент отмечен
        if item.checkState(0) == Qt.CheckState.Checked:
            file_path = item.data(0, Qt.ItemDataRole.UserRole)
            if file_path and os.path.isfile(file_path):
                files.append(file_path)
        # Если элемент частично отмечен (только для папок) – можно игнорировать или обрабатывать как папку
        # Но мы будем обрабатывать только явно отмеченные файлы.
        # Если отмечена папка, добавим все файлы из неё рекурсивно.
        elif item.checkState(0) == Qt.CheckState.Checked and os.path.isdir(item.data(0, Qt.ItemDataRole.UserRole)):
            # Если папка отмечена, добавим все файлы внутри (рекурсивно)
            folder = item.data(0, Qt.ItemDataRole.UserRole)
            for root, dirs, files_in_folder in os.walk(folder):
                for f in files_in_folder:
                    if f.lower().endswith('.xlsx'):
                        files.append(os.path.join(root, f))
            # Дочерние элементы не нужно обходить отдельно, так как мы уже прошли всю папку.
            # Но чтобы избежать дублирования, пропускаем детей.
            return

        # Обходим детей
        for i in range(item.childCount()):
            self.get_checked_files(item.child(i), files)

    def process_selected_files(self):
        """Собирает отмеченные файлы и запускает их обработку."""
        files = self.get_checked_files()
        if not files:
            QMessageBox.information(self, "Ничего не выбрано", "Не выбрано ни одного файла для обработки.")
            return
        # Вызываем существующий process_files
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
    # ===========================================================================
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

    # =================================================================================================
    # Обновлённый тулбар (убираем старые кнопки)
    def create_toolbar(self):
        toolbar = QToolBar("Главная панель")
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(24, 24))
        self.addToolBar(toolbar)

        # Убраны кнопки загрузки файлов и папки

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

        # Кнопка настроек
        settings_action = QAction("⚙️ Настройки", self)
        settings_action.triggered.connect(self.show_settings)
        toolbar.addAction(settings_action)

        # Кнопка "О программе"
        about_action = QAction("ℹ️ О программе", self)
        about_action.triggered.connect(self.show_about)
        toolbar.addAction(about_action)


    def show_settings(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("Настройки")
        dialog.setModal(True)
        layout = QVBoxLayout(dialog)

        # Папка для загрузки
        load_layout = QHBoxLayout()
        load_layout.addWidget(QLabel("Папка для загрузки:"))
        self.load_folder_edit = QLineEdit()
        load_layout.addWidget(self.load_folder_edit)
        load_btn = QPushButton("Обзор...")
        load_btn.clicked.connect(lambda: self._choose_folder(self.load_folder_edit))
        load_layout.addWidget(load_btn)
        layout.addLayout(load_layout)

        # Папка для выгрузки
        export_layout = QHBoxLayout()
        export_layout.addWidget(QLabel("Папка для выгрузки:"))
        self.export_folder_edit = QLineEdit()
        export_layout.addWidget(self.export_folder_edit)
        export_btn = QPushButton("Обзор...")
        export_btn.clicked.connect(lambda: self._choose_folder(self.export_folder_edit))
        export_layout.addWidget(export_btn)
        layout.addLayout(export_layout)

        # Кнопки ОК/Отмена
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box.accepted.connect(dialog.accept)
        btn_box.rejected.connect(dialog.reject)
        layout.addWidget(btn_box)

        dialog.exec()

    # ==================================================================================================
    # """Загрузка начальных демо-данных"""
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

    def _choose_folder(self, line_edit):
        folder = QFileDialog.getExistingDirectory(self, "Выберите папку")
        if folder:
            line_edit.setText(folder)

    def load_files(self):
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, "Выберите файлы Excel", "", "Excel Files (*.xlsx)"
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
                if file.lower().endswith('.xlsx'):
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
    
    # Главная цифра НДС к уплате
    def get_vat_summary(self, date_from=None, date_to=None, company=None):

        query = """
            SELECT 
                SUM(vat_to_budget) as vat_output,
                SUM(vat_deductible) as vat_input
            FROM reports
            WHERE 1=1
        """

        params = []

        if company:
            query += " AND company = ?"
            params.append(company)

        if date_from:
            query += " AND period_start >= ?"
            params.append(date_from)

        if date_to:
            query += " AND period_end <= ?"
            params.append(date_to)

        result = pd.read_sql_query(query, self.conn, params=params)

        vat_output = result['vat_output'].iloc[0] or 0
        vat_input = result['vat_input'].iloc[0] or 0

        return {
            "vat_output": vat_output,
            "vat_input": vat_input,
            "vat_payable": vat_output - vat_input
        }
    
    
    def _extract_period_dates(self, text):
        """
        Извлекает дату начала и окончания периода из текста книги покупок/продаж.
        Возвращает (date_start, date_end) в формате YYYY-MM-DD
        """
        import re
        from datetime import datetime

        # Ищем две даты вида 01.04.2025
        dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', text)

        if len(dates) >= 2:
            date_start = datetime.strptime(dates[0], "%d.%m.%Y").strftime("%Y-%m-%d")
            date_end = datetime.strptime(dates[1], "%d.%m.%Y").strftime("%Y-%m-%d")
            return date_start, date_end

        # Если не нашли — возвращаем None
        return None, None
    
    # ==============================================================================================================  
    # Импорт эксель файлов

    def _import_excel_file(self, file_path):

        if file_path.lower().endswith('.xlsx'):
            try:
                import xlrd
            except ImportError:
                raise ImportError("Для чтения файлов .xls установите xlrd: pip install xlrd")

        try:
            df_preview = pd.read_excel(file_path, nrows=10, header=None, dtype=str)
        except:
            df_preview = pd.read_excel(file_path, nrows=10, header=None)
            df_preview = df_preview.astype(str)

        df_preview = df_preview.fillna('')
        preview_text = ' '.join(df_preview.values.flatten()).lower()

        # ----------- КНИГИ -----------
        if 'книга покупок' in preview_text:
            df = self._parse_purchase_book(file_path)
            return self.db.save_data(df)

        elif 'книга продаж' in preview_text:
            df = self._parse_sales_book(file_path)
            return self.db.save_data(df)

        # ----------- ОСВ -----------
        elif 'оборотно-сальдовая ведомость по счету 19' in preview_text or 'анализ счета 19' in preview_text:
            df = self._parse_osv_19_detailed(file_path)
            return self.db.save_data(df)

        elif 'оборотно-сальдовая ведомость по счету 41' in preview_text:
            df = self._parse_osv_41_detailed(file_path)
            return self.db.save_data(df)

        elif 'оборотно-сальдовая ведомость по счету 44' in preview_text:
            df = self._parse_osv_44_detailed(file_path)
            return self.db.save_data(df)

        elif 'оборотно-сальдовая ведомость по счету 60' in preview_text:
            df = self._parse_osv_60_detailed(file_path)
            return self.db.save_data(df)

        elif 'оборотно-сальдовая ведомость по счету 62' in preview_text:
            df = self._parse_osv_62_detailed(file_path)
            return self.db.save_data(df)

        elif 'оборотно-сальдовая ведомость по счету 68' in preview_text:
            df = self._parse_osv_68_detailed(file_path)
            return self.db.save_data(df)

        elif 'оборотно-сальдовая ведомость по счету 90' in preview_text:
            df = self._parse_osv_90_detailed(file_path)
            return self.db.save_data(df)

        elif 'оборотно-сальдовая ведомость по счету 91' in preview_text:
            df = self._parse_osv_91_detailed(file_path)
            return self.db.save_data(df)

        elif 'отчет по продажам' in preview_text:
            return self._parse_sales_report_detailed(file_path)
            
        else:
            return self._import_legacy(file_path)
    
    #======================================================================================================================
    # Расчет НДС за период
    # НДС к уплате = Σ НДС начисленный (продажи) – Σ НДС к вычету (покупки)

    def calculate_vat_for_period(self, company, period):

        df = self.db.get_data(company=company, period=period)

        purchase_vat = df[
            df["doc_type"] == "purchase_book"
        ]["vat_amount"].sum()

        sales_vat = df[
            df["doc_type"] == "sales_book"
        ]["vat_amount"].sum()

        return {
            "vat_output": sales_vat,
            "vat_input": purchase_vat,
            "vat_payable": sales_vat - purchase_vat
        }

    # ========== Отдельный парсер ОСВ 68  ==========    
    def _parse_osv_68_detailed(self, file_path):

        import pandas as pd
        import re

        df = pd.read_excel(file_path, header=None)

        records = []
        company = None
        period_start = None
        period_end = None

        # компания
        for i in range(10):
            cell = str(df.iloc[i, 0])
            if "ООО" in cell:
                company = cell.strip()
                break

        # период
        for i in range(20):
            cell = str(df.iloc[i, 0])
            match_year = re.search(r'за\s+(\d{4})\s*г', cell)
            if match_year:
                year = int(match_year.group(1))
                period_start = f"{year}-01-01"
                period_end = f"{year}-12-31"
                break

        # ищем строку "68"
        for idx in range(len(df)):
            name = str(df.iloc[idx, 0]).strip()

            if name == "68":

                debit = self._clean_number(df.iloc[idx, 3])
                credit = self._clean_number(df.iloc[idx, 4])

                records.append({
                    "company": company,
                    "account": "68",
                    "counterparty": "Налоги",
                    "debit_turnover": float(debit),
                    "credit_turnover": float(credit),
                    "period_start": period_start,
                    "period_end": period_end,
                    "doc_type": "osv_68"
                })
                break

        return pd.DataFrame(records)
    
    # ==================================================================================
    # ========== Идеальный универсальный парсер ОСВ (19/41/44/60/62/90/91)  ==========    
     
    def _parse_osv_generic(self, file_path, account_number):

        import pandas as pd
        import re
        from datetime import datetime, date

        df = pd.read_excel(file_path, header=None)
        df = df.fillna("")

        records = []

        company = None
        period_start = None
        period_end = None
        current_account = None

        # =========================================
        # 1. Определяем компанию
        # =========================================
        for i in range(min(15, len(df))):
            cell = str(df.iloc[i, 0]).strip()
            if "ООО" in cell.upper():
                company = cell
                break

        if not company:
            company = "Неизвестная компания"

        # =========================================
        # 2. Определяем период
        # =========================================
        header_text = " ".join(
            df.iloc[:20].astype(str).values.flatten()
        ).lower()

        # 2.1 Период вида: с 01.01.2025 по 31.03.2025
        match_period = re.search(
            r'с\s*(\d{2}\.\d{2}\.\d{4})\s*по\s*(\d{2}\.\d{2}\.\d{4})',
            header_text
        )

        if match_period:
            period_start = datetime.strptime(
                match_period.group(1),
                "%d.%m.%Y"
            ).strftime("%Y-%m-%d")

            period_end = datetime.strptime(
                match_period.group(2),
                "%d.%m.%Y"
            ).strftime("%Y-%m-%d")

        else:
            # 2.2 Годовая форма: за 2025 г., за 2025 год
            match_year = re.search(
                r'за\s*(\d{4})\s*(г\.?|год)?',
                header_text
            )

            if match_year:
                year = int(match_year.group(1))
                period_start = f"{year}-01-01"
                period_end = f"{year}-12-31"
            else:
                raise Exception("Не найден период в ОСВ")

        # =========================================
        # 3. Определяем колонки дебет/кредит
        # =========================================
        debit_col = None
        credit_col = None

        for i in range(len(df)):
            row = [str(x).lower() for x in df.iloc[i]]

            if "оборот" in " ".join(row) and "дебет" in " ".join(row):
                for j, val in enumerate(row):
                    if "дебет" in val:
                        debit_col = j
                    if "кредит" in val:
                        credit_col = j
                break

        # fallback (стандартная структура 1С)
        max_col = df.shape[1] - 1
        if debit_col is None or debit_col > max_col:
            debit_col = 3 if 3 <= max_col else 0  # или другой fallback
        if credit_col is None or credit_col > max_col:
            credit_col = 4 if 4 <= max_col else 1

        # =========================================
        # 4. Основной цикл по строкам
        # =========================================
        for idx in range(len(df)):

            name = str(df.iloc[idx, 0]).strip()

            if not name or name.lower() == "nan":
                continue

            name_lower = name.lower()

            # если строка — это номер счета
            if re.fullmatch(r'\d+(\.\d+)?', name):
                current_account = name
                continue

            # стоп на "Итого"
            if name_lower.startswith("итого"):
                break

            if not current_account:
                continue

            # ИСПРАВЛЕНИЕ: проверяем, что индексы колонок существуют
            debit_raw = df.iloc[idx, debit_col] if debit_col < len(df.columns) else 0
            credit_raw = df.iloc[idx, credit_col] if credit_col < len(df.columns) else 0

            debit = self._clean_number(debit_raw)
            credit = self._clean_number(credit_raw)

            if debit == 0 and credit == 0:
                continue

            records.append({
                "company": company,
                "account": current_account,
                "counterparty": name,
                "debit_turnover": float(debit),
                "credit_turnover": float(credit),
                "period_start": period_start,
                "period_end": period_end,
                "doc_type": f"osv_{account_number}"
            })

        result_df = pd.DataFrame(records)

        print(f"ОСВ {account_number}: найдено записей — {len(result_df)}")

        return result_df
    
    # =========================================================
    # Обертки для универсального парсера ОСВ

    def _parse_osv_19_detailed(self, file_path):
        return self._parse_osv_generic(file_path, 19)

    def _parse_osv_41_detailed(self, file_path):
        return self._parse_osv_generic(file_path, 41)

    def _parse_osv_44_detailed(self, file_path):
        return self._parse_osv_generic(file_path, 44)

    def _parse_osv_60_detailed(self, file_path):
        return self._parse_osv_generic(file_path, 60)

    def _parse_osv_62_detailed(self, file_path):
        return self._parse_osv_generic(file_path, 62)

    def _parse_osv_90_detailed(self, file_path):
        return self._parse_osv_generic(file_path, 90)

    def _parse_osv_91_detailed(self, file_path):
        return self._parse_osv_generic(file_path, 91)
      
    # =============================================================
    #  ============  НОВЫЕ КНИГИ ПОКУПОК М ПРОДАЖ

    # =============================================================
    #  ============  НОВАЯ КНИГА ПОКУПОК ==============================

    
    def _parse_purchase_book(self, file_path):
        import pandas as pd
        import re
        from datetime import datetime

        df = pd.read_excel(file_path, header=None, dtype=str)
        df = df.fillna('').astype(str).apply(lambda col: col.str.strip())

        # Извлекаем компанию и период
        header_text = ' '.join(df.iloc[:20].values.flatten()).lower()
        company_match = re.search(r'покупатель\s+([^\d]+?)(?:\d|$)', header_text, re.IGNORECASE)
        company = company_match.group(1).strip() if company_match else "Неизвестная компания"

        period_match = re.search(r'с\s+(\d{2}\.\d{2}\.\d{4})\s+по\s+(\d{2}\.\d{2}\.\d{4})', header_text, re.IGNORECASE)
        if not period_match:
            raise ValueError("Не найден период в книге покупок")
        period_start = datetime.strptime(period_match.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
        period_end = datetime.strptime(period_match.group(2), "%d.%m.%Y").strftime("%Y-%m-%d")

        # Поиск строки с номерами колонок
        header_row_idx = None
        num_to_idx = {}
        for i in range(len(df)):
            row = df.iloc[i].tolist()
            # Собираем индексы, где значение является числом (целым)
            num_positions = []
            numbers_found = []
            for j, val in enumerate(row):
                if val.strip().isdigit():
                    num = int(val)
                    num_positions.append((j, num))
                    numbers_found.append(num)
            # Проверяем, есть ли последовательность 1,2,3,... подряд
            if numbers_found:
                # Ищем самую длинную возрастающую последовательность с шагом 1
                # Упростим: проверим, есть ли в строке число 1, и затем последовательно ищем 2,3,... пока они есть
                # Для этого отсортируем num_positions по индексу (они и так в порядке возрастания)
                # Но числа могут идти не подряд из-за пропусков. Однако в нашем случае они должны быть подряд.
                # Проверим, что числа идут по порядку: первое число должно быть 1, второе 2 и т.д.
                start_idx = None
                expected = 1
                found_seq = []
                for pos, num in num_positions:
                    if num == expected:
                        found_seq.append((pos, num))
                        expected += 1
                    else:
                        # Если не совпало, сбрасываем
                        found_seq = []
                        expected = 1
                        if num == 1:
                            found_seq.append((pos, num))
                            expected = 2
                if len(found_seq) >= 5:  # нашли хотя бы 5 подряд идущих чисел
                    header_row_idx = i
                    for pos, num in found_seq:
                        num_to_idx[num] = pos
                    break

        if header_row_idx is None:
            raise ValueError("Не найдена строка с номерами колонок")

        # Проверяем наличие нужных номеров
        required = [9, 14, 15]
        for num in required:
            if num not in num_to_idx:
                raise ValueError(f"Не найден номер колонки {num} в строке заголовка")

        seller_col = num_to_idx[9]
        amount_col = num_to_idx[14]
        vat_col = num_to_idx[15]

        # Сбор данных
        records = []
        current_seller = None
        data_start = header_row_idx + 1

        for i in range(data_start, len(df)):
            row = df.iloc[i].tolist()
            first_cell = row[0].strip().lower() if len(row) > 0 else ''

            if not first_cell:
                continue

            # Проверка на итоговые строки
            if 'всего' in first_cell:
                # Если "всего по продавцу" - возможно, сбрасываем продавца и продолжаем
                # Но в данных после "всего по продавцу" может идти новый продавец, поэтому не прерываем, а просто сбрасываем
                if 'по продавцу' in first_cell:
                    current_seller = None
                    continue
                else:
                    # Это финальное "Всего" - заканчиваем
                    break

            # Проверяем, является ли первая ячейка числом (номер строки)
            # Номер строки может быть целым числом (например, "1", "2")
            if first_cell.replace('.', '', 1).replace(',', '').isdigit():
                # Это строка данных
                seller = current_seller
                # Если в колонке продавца что-то есть (не пусто и не равно текущему продавцу), используем его
                if seller_col < len(row) and row[seller_col].strip():
                    seller = row[seller_col].strip()
                    current_seller = seller
                elif not seller:
                    # Если продавец не определён, пропускаем (обычно не должно быть)
                    continue

                amount_str = row[amount_col] if amount_col < len(row) else '0'
                vat_str = row[vat_col] if vat_col < len(row) else '0'
                amount = self._clean_number(amount_str)
                vat = self._clean_number(vat_str)

                if amount == 0 and vat == 0:
                    continue

                records.append({
                    'company': company,
                    'period_start': period_start,
                    'period_end': period_end,
                    'product_group': 'Покупки',
                    'nomenclature': seller,
                    'revenue': 0.0,
                    'vat_in_revenue': 0.0,
                    'cost_price': amount,
                    'gross_profit': 0.0,
                    'sales_expenses': 0.0,
                    'other_income_expenses': 0.0,
                    'net_profit': 0.0,
                    'vat_deductible': vat,
                    'vat_to_budget': 0.0,
                    'quantity': 1
                })
            else:
                # Если первая ячейка не число, возможно это название нового продавца
                # Но нужно исключить строки с кодами (например, "008", "010"), которые тоже могут быть не числами? Они выглядят как трёхзначные числа, но могут быть с лидирующими нулями, и isdigit() вернёт True? На самом деле "008".isdigit() вернёт True, потому что строка состоит из цифр. Значит, они попадут в ветку с числом. Но нам нужно их пропустить, потому что это не номера строк. Как отличить? Номер строки обычно однозначное или двузначное число, а коды вида "008" – трёхзначные с ведущим нулём. Можно проверить длину: если длина строки > 2 и она состоит из цифр, возможно это код операции. Но лучше добавить проверку, что если это число и оно меньше 100, то считаем номером строки, иначе пропускаем.
                # Однако для простоты можно просто игнорировать строки, где первая ячейка состоит из трёх цифр (код) – они идут сразу после заголовка. Мы можем пропустить все строки до тех пор, пока не встретим первую строку с названием продавца или с номером строки. Но у нас есть явные строки с продавцом (например, "ООО КОМПАНИЯ"), которые не являются числами. Поэтому:
                if not first_cell[0].isdigit():
                    # Это не число, значит вероятно название продавца
                    current_seller = row[0].strip()
                # Если это число, но не подходит под номер строки (например, трёхзначный код), мы ничего не делаем (пропускаем)

        if not records:
            raise ValueError("Не найдено записей в книге покупок")

        print(f"Книга покупок: найдено записей — {len(records)}")
        return pd.DataFrame(records)
    
    # ===============================================================================================================
    # ============================= НОВАЯ КНИГА ПРОДАЖ ======================================:
    
    def _parse_sales_book(self, file_path):
        import pandas as pd
        import re
        from datetime import datetime

        df = pd.read_excel(file_path, header=None, dtype=str)
        df = df.fillna('').astype(str).apply(lambda col: col.str.strip())
        if df.empty:
            raise ValueError("Файл книги продаж пуст")

        # --- Извлечение компании и периода ---
        header_text = ' '.join(df.iloc[:15].values.flatten()).lower()
        
        # Продавец
        company_match = re.search(r'продавец\s+([^\d]+?)(?:\d|$)', header_text, re.IGNORECASE)
        company = company_match.group(1).strip() if company_match else "Неизвестно"

        period_match = re.search(r'с\s+(\d{2}\.\d{2}\.\d{4})\s+по\s+(\d{2}\.\d{2}\.\d{4})', header_text, re.IGNORECASE)
        if not period_match:
            raise ValueError("Не найден период в книге продаж")
        period_start = datetime.strptime(period_match.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
        period_end = datetime.strptime(period_match.group(2), "%d.%m.%Y").strftime("%Y-%m-%d")

        # --- Поиск строки с номерами колонок ---
        header_row = None
        for i in range(len(df)):
            row = df.iloc[i].tolist()
            # Проверяем последовательность 1,2,3 в начале
            if len(row) > 2 and row[0] == '1' and row[1] == '2' and row[2] == '3':
                header_row = i
                break

        if header_row is None:
            raise ValueError("Не найдена строка с нумерацией колонок в книге продаж")

        # --- Заголовки на предыдущей строке ---
        title_row = header_row - 1
        if title_row < 0:
            raise ValueError("Строка с номерами колонок слишком близко к началу")
        headers = df.iloc[title_row].tolist()
        headers_lower = [str(h).lower() for h in headers]

        # Определяем индексы нужных колонок
        col_buyer = None
        col_amount_with_vat = None  # стоимость с НДС (по данным в колонке 14)
        col_vat = None              # сумма НДС (по данным в колонке 17)

        # Ищем по ключевым словам
        for idx, h in enumerate(headers_lower):
            if 'наименование покупателя' in h:
                col_buyer = idx
            # Для суммы с НДС: ищем "стоимость продаж" и, возможно, "включая ндс", но в шапке сложно
            # Поэтому будем искать "стоимость продаж" и возьмём первую попавшуюся, но уточним
            if 'стоимость продаж' in h and col_amount_with_vat is None:
                col_amount_with_vat = idx
            if 'сумма ндс' in h and col_vat is None:
                col_vat = idx

        # Если не нашли, используем фиксированные индексы (основываясь на примере)
        if col_buyer is None:
            # Покупатель обычно в колонке 7 (индекс 6)
            col_buyer = 6
        if col_amount_with_vat is None:
            # Сумма с НДС в колонке 14 (индекс 13)
            col_amount_with_vat = 13
        if col_vat is None:
            # НДС в колонке 17 (индекс 16)
            col_vat = 16

        # --- Сбор записей ---
        records = []
        current_buyer = None
        start_row = header_row + 1

        for i in range(start_row, len(df)):
            row = df.iloc[i].tolist()
            if not row or not row[0]:
                continue

            first = row[0].strip().lower()
            if 'всего' in first:
                break

            # Проверяем, является ли первый элемент числом (номер строки)
            if re.match(r'^\d+$', first):
                if current_buyer is None:
                    continue

                amount = self._clean_number(row[col_amount_with_vat] if col_amount_with_vat < len(row) else '0')
                vat = self._clean_number(row[col_vat] if col_vat < len(row) else '0')

                if amount == 0 and vat == 0:
                    continue

                records.append({
                    'company': company,
                    'period_start': period_start,
                    'period_end': period_end,
                    'product_group': 'Продажи',
                    'nomenclature': current_buyer,
                    'revenue': amount,
                    'vat_in_revenue': vat,
                    'cost_price': 0.0,
                    'gross_profit': 0.0,
                    'sales_expenses': 0.0,
                    'other_income_expenses': 0.0,
                    'net_profit': 0.0,
                    'vat_deductible': 0.0,
                    'vat_to_budget': vat,
                    'quantity': 1
                })
            else:
                # Это новый покупатель
                if first and not first[0].isdigit():
                    current_buyer = row[0].strip()
                    continue

        if not records:
            raise ValueError("Не найдено записей в книге продаж")

        return pd.DataFrame(records)

    

    # ========== ОТЧЁТ ПО ПРОДАЖАМ (по товарам и месяцам) ==========
    def _parse_sales_report_detailed(self, file_path):
        df = pd.read_excel(file_path, header=None)
        header_text = self._flatten_text(df, slice(0, 5))
        company = self._extract_company_from_text(header_text)
        period_base = self._extract_period_from_text(header_text, file_path)
        year = period_base.split('.')[1] if '.' in period_base else period_base
        if df.empty:
            raise ValueError("Файл пустой или данные не распознаны")
        print(file_path)
        print(df.head())
        print(df.shape)
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
                self, "Выберите файл Excel", "", "Excel Files (*x *.xlsx)"
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
        """Отображает DataFrame в таблице с фиксированным порядком колонок"""
        self.table_model.setRowCount(0)
        
        # Фиксированный порядок колонок (английские имена)
        column_order = [
            'period', 'company', 'product_group', 'nomenclature',
            'revenue', 'vat_in_revenue', 'cost_price', 'gross_profit',
            'sales_expenses', 'other_income_expenses', 'net_profit',
            'vat_deductible', 'vat_to_budget', 'quantity', 'import_date'
        ]
        
        # Соответствие русских названий
        ru_headers = {
            'period': 'Период',
            'company': 'Компания',
            'product_group': 'Товарная группа',
            'nomenclature': 'Номенклатура',
            'revenue': 'Выручка (с НДС)',
            'vat_in_revenue': 'НДС в выручке',
            'cost_price': 'Себестоимость',
            'gross_profit': 'Валовая прибыль',
            'sales_expenses': 'Расходы на продажу',
            'other_income_expenses': 'Прочие доходы/расходы',
            'net_profit': 'Чистая прибыль',
            'vat_deductible': 'НДС к вычету',
            'vat_to_budget': 'НДС К УПЛАТЕ',
            'quantity': 'Оборот (кол-во)',
            'import_date': 'Дата импорта'
        }
        
        # Устанавливаем заголовки
        headers = [ru_headers.get(col, col) for col in column_order]
        self.table_model.setHorizontalHeaderLabels(headers)
        
        if df is None or df.empty:
            return
        
        # Проходим по строкам DataFrame
        for _, row in df.iterrows():
            items = []
            for col in column_order:
                value = row[col] if col in row.index else ''
                # Форматирование для числовых колонок
                if col in ['revenue', 'vat_in_revenue', 'cost_price', 'gross_profit',
                        'sales_expenses', 'other_income_expenses', 'net_profit',
                        'vat_deductible', 'vat_to_budget']:
                    if isinstance(value, (int, float)):
                        display_value = f"{value:,.0f} ₽".replace(",", " ")
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
                item.setData(value)  # сохраняем исходное значение для сортировки
                items.append(item)
            self.table_model.appendRow(items)
        
        # Автоматическая подгонка ширины колонок
        self.table_view.resizeColumnsToContents()


    # """Применение фильтров"""
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
        total_revenue = 0
        total_vat = 0
        total_profit = 0

        if self.current_df is not None and not self.current_df.empty:
            for col in ['revenue', 'vat_to_budget', 'net_profit']:
                if col in self.current_df.columns:
                    self.current_df[col] = pd.to_numeric(
                        self.current_df[col], errors='coerce'
                    ).fillna(0)

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
                    colors = plt.cm.tab10(np.linspace(0, 1, len(company_vat)))
                    bars = self.axes[0, 1].bar(company_vat.index, company_vat.values, color=colors)
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
                # period_revenue = df_clean.groupby('period')['revenue'].sum().sort_index()
                period_revenue = (
                    df_clean
                    .groupby('period')['revenue']
                    .sum()
                    .reset_index()
                )

                period_revenue['period_dt'] = pd.to_datetime(
                    '01.' + period_revenue['period'],
                    format='%d.%m.%Y',
                    errors='coerce'
                )

                period_revenue = period_revenue.sort_values('period_dt')

                if not period_revenue.empty and period_revenue['revenue'].sum() != 0:
                    self.axes[1, 0].plot(
                        period_revenue['period_dt'],
                        period_revenue['revenue'],
                        marker='o',
                        linewidth=2,
                        color='#9b59b6'
                    )
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
        <p><b>Версия программы:</b> v5.10.0</p>
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