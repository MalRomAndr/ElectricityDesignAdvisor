# Схема исходных данных
Исходные данные хранятся в файле database.db
## `Parts` - таблица деталей
- `HeadingId` - Id заголовка/подзаголовка таблицы спецификации
- `CategoryId` - Id категрии товара в справочнике
- `Id` - Уникальное наименование детали (артикул)
- `Name` - Описание детали
- `Code` - Код товара по какому-либо стороннему справочнику (например GTIN)
- `Code_ru_pr_tat` - Код детали по справочнику в Татарстане
- `SupplierId` - Id поставщика
- `UnitId` - Единица измерения
- `Weight` - Масса
- `IsVisible` - Настройка видимости глобальная

## `Headings` - заголовки разделов/подразделов спецификации
- `Id` - Id категрии
- `Name` - Имя категории
- `Order` - Порядок сортировки категорий при формировании отчета

## `Categories` - категории товаров в справочнике
- `Id`
- `ParentId` - Id родительской категории, ParentId=0 для категории верхнего уровня
- `Name` - Имя категории
- `UseInLop` - Товар из этой категории используется в формировании ведомости опор
- `IsVisible` - Настройка видимости глобальная

## `StandardProjects` - типовые проекты (сборники типовых опор), отображаются в интерфейсе в виде папок
- `Id`
- `ParentId` - Id родительской папки
- `Number` - Шифр проекта
- `Name` - Наименование
- `UseInLop` - _True_ - типовой проект, _False_ - просто папка
- `ImageIndex` - Иконка папки в интерфейсе
- `IsVisible` -  - Настройка видимости глобальная

## `Structures` - сборная контрукция (опора или какой-то другой объект)
- `Id` - Марка (буквенно-цифровой шифр)
- `Name` - Наименование
- `StandardProjectId` - Id папки, в которой хранится
- `TypeId` - Тип объекта
- `PlanSymbol` - Условное графическое обозначение на плане
- `SchemeSymbol` - Условное графическое обозначение на схеме
- `DrawingNumber` - Номер чертежа из типового проекта
- `Image` - Ссылка на превью картинку

## `ObjectTypes` - типы объектов
- `Id`
- `Name` - Имя
- `HeadingId` - Id заголовка/подзаголовка таблицы спецификации
- `Order` - Порядок сортировки при формировании отчетов
- `IsPole` - опора или нет

## `StructuresParts` - промежуточная таблица для сопоставления опор и их составных деталей
- `StructureId` - Id опоры или какой-то другой сборной конструкции
- `PartId` - Id детали
- `Quantity` - Количество
- `Comment` - Комментарий

## `Suppliers` - Поставщики
- `Supplier` - Наименование организации

## `Units` - Единицы измерения
- `Unit` - Единица измерения

## `StructuresLinks` - Таблица со ссылками на дополнительные файлы, привязанные к опоре

## `Conductors` - Если деталь (Part) является проводом, то в этой таблице хранятся дополнительные характеристики для такого типа объектов