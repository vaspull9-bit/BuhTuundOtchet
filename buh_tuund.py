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

# ==================== БАЗА ДАННЫХ ====================
class DatabaseManager:
    def __init__(self):
        self.conn = sqlite3.connect('buh_tuund.db', check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        cursor = self.conn.cursor()
        # Основная таблица данных
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
        
        # Таблица истории импорта
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
        cursor = self.conn.cursor()
        records = df.to_records(index=False)
        
        for record in records:
            cursor.execute('''
                INSERT INTO reports 
                (period, company, product_group, nomenclature, revenue, vat_in_revenue,
                 cost_price, gross_profit, sales_expenses, other_income_expenses,
                 net_profit, vat_deductible, vat_to_budget, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', tuple(record))
        
        cursor.execute('''
            INSERT INTO import_history (filename, records_count) 
            VALUES (?, ?)
        ''', ('Импорт данных', len(df)))
        
        self.conn.commit()
        return len(df)
    
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
    
    def create_toolbar(self):
        toolbar = QToolBar("Главная панель")
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(24, 24))
        self.addToolBar(toolbar)
        
        # Кнопка загрузки Excel
        load_action = QAction(QIcon.fromTheme("document-open"), "Загрузить Excel", self)
        load_action.triggered.connect(self.load_excel)
        load_action.setShortcut("Ctrl+O")
        toolbar.addAction(load_action)
        
        toolbar.addSeparator()
        
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
    
    def load_excel(self):
        """Загрузка данных из Excel файла с поддержкой русских названий колонок"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Выберите файл Excel", "", "Excel Files (*.xlsx *.xls)"
        )

        if not file_path:
            return

        try:
            df = pd.read_excel(file_path)

            # Словарь соответствия русских и английских названий колонок
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

            # Переименовываем русские колонки в английские (если они есть)
            df.rename(columns=lambda x: column_mapping.get(str(x).strip(), str(x).strip()), inplace=True)

            # Проверяем наличие основных английских колонок
            required_columns = ['period', 'company', 'product_group', 'nomenclature',
                                'revenue', 'vat_in_revenue', 'cost_price', 'vat_to_budget', 'quantity']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                QMessageBox.warning(self, "Ошибка",
                                    f"В файле отсутствуют обязательные колонки (или их русские аналоги): {', '.join(missing_columns)}")
                return

            # Расчет дополнительных полей, если их нет
            if 'gross_profit' not in df.columns:
                df['gross_profit'] = df['revenue'] - df['vat_in_revenue'] - df['cost_price']

            if 'net_profit' not in df.columns:
                df['net_profit'] = df['gross_profit']
                if 'sales_expenses' in df.columns:
                    df['net_profit'] -= df['sales_expenses']
                if 'other_income_expenses' in df.columns:
                    df['net_profit'] += df['other_income_expenses']

            # Если нет колонок расходов, создаем с нулями
            if 'sales_expenses' not in df.columns:
                df['sales_expenses'] = 0
            if 'other_income_expenses' not in df.columns:
                df['other_income_expenses'] = 0
            if 'vat_deductible' not in df.columns:
                df['vat_deductible'] = 0

            # Сохранение в базу данных
            records_count = self.db.save_data(df)

            # Обновление текущего DataFrame и отображения
            self.current_df = df
            self.display_data(df)
            self.update_totals()
            self.update_charts()  # Важно: обновляем графики после загрузки

            QMessageBox.information(self, "Успех",
                                    f"Загружено {records_count} записей из файла: {os.path.basename(file_path)}")

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при загрузке файла: {str(e)}")
    
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
        """Обновление итоговых значений"""
        if self.current_df is not None and not self.current_df.empty:
            total_revenue = self.current_df['revenue'].sum()
            total_vat = self.current_df['vat_to_budget'].sum()
            total_profit = self.current_df['net_profit'].sum()
            
            self.revenue_label.setText(f"Выручка: {total_revenue:,.0f} ₽".replace(",", " "))
            self.vat_label.setText(f"НДС к уплате: {total_vat:,.0f} ₽".replace(",", " "))
            self.profit_label.setText(f"Чистая прибыль: {total_profit:,.0f} ₽".replace(",", " "))
    
    def update_charts(self):
        """Обновление графиков"""
        if self.current_df is None or self.current_df.empty:
            return
        
        # Очистка предыдущих графиков
        for ax in self.axes.flat:
            ax.clear()
        
        # 1. Круговая диаграмма по товарным группам
        group_profit = self.current_df.groupby('product_group')['net_profit'].sum()
        colors1 = plt.cm.Set3(np.linspace(0, 1, len(group_profit)))
        self.axes[0, 0].pie(group_profit.values, labels=group_profit.index, autopct='%1.1f%%', 
                           colors=colors1, startangle=90)
        self.axes[0, 0].set_title('Распределение прибыли по товарным группам')
        
        # 2. Столбчатая диаграмма НДС по компаниям
        company_vat = self.current_df.groupby('company')['vat_to_budget'].sum()
        bars = self.axes[0, 1].bar(company_vat.index, company_vat.values, 
                                   color=['#3498db', '#2ecc71', '#e74c3c'])
        self.axes[0, 1].set_title('НДС к уплате по компаниям')
        self.axes[0, 1].set_ylabel('Сумма НДС, ₽')
        self.axes[0, 1].tick_params(axis='x', rotation=45)
        
        # Добавление значений над столбцами
        for bar in bars:
            height = bar.get_height()
            self.axes[0, 1].text(bar.get_x() + bar.get_width()/2., height + max(company_vat.values)*0.01,
                                f'{height:,.0f}'.replace(",", " "), ha='center', va='bottom')
        
        # 3. Линейный график выручки по периодам
        if 'period' in self.current_df.columns:
            period_revenue = self.current_df.groupby('period')['revenue'].sum().sort_index()
            self.axes[1, 0].plot(period_revenue.index, period_revenue.values, 
                                marker='o', linewidth=2, color='#9b59b6')
            self.axes[1, 0].set_title('Динамика выручки по периодам')
            self.axes[1, 0].set_ylabel('Выручка, ₽')
            self.axes[1, 0].grid(True, alpha=0.3)
            self.axes[1, 0].tick_params(axis='x', rotation=45)
        
        # 4. ТОП-5 товаров по прибыльности
        top_products = self.current_df.nlargest(5, 'net_profit')[['nomenclature', 'net_profit']]
        bars2 = self.axes[1, 1].barh(top_products['nomenclature'], top_products['net_profit'],
                                    color=plt.cm.viridis(np.linspace(0.2, 0.8, len(top_products))))
        self.axes[1, 1].set_title('ТОП-5 товаров по прибыльности')
        self.axes[1, 1].set_xlabel('Прибыль, ₽')
        
        # Автонастройка макета
        plt.tight_layout()
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
        <p><b>Версия программы:</b> v1.1.0</p>
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