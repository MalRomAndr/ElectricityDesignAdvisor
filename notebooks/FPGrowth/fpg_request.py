import pandas as pd
import gradio as gr
import math
import random

df = pd.read_pickle('fpg_df.pickle')
rules = pd.read_pickle('fpg_rules.pickle')


def get_results(request):
    request = df[df['StructureId'] == request]
    results = pd.DataFrame()
    for _ in range(request.shape[0]):
        # Берем Id одного элемента из запроса
        element = request.iloc[_]['Id']

        # Находим все ассоциации по этому элементу
        answer = rules[rules['antecedents'] == element]

        # Проверяем, чтобы предлагаемые элементы не содержались в запросе, сортируем Id
        answer = answer[~answer['consequents'].isin(request['Id'])].sort_values(by='consequents')

        # Находим результат по найденным предложениям, проверяем совпадение по TypeId
        result = df[(df['Id'].isin(answer['consequents'])) & (df['TypeId'] == request.iloc[_]['TypeId'])]

        # Удаляем дубликаты, сортируем по Id
        result = result.drop_duplicates(subset=['Id']).sort_values(by='Id')

        # Приводим ответ в соответствие с отфильтрованным результатом
        answer = answer[answer['consequents'].isin(result['Id'])]

        # Добавляем в таблицу с результатом метрику из ответа
        result['metric'] = list((answer['confidence']*0.55 + answer['consequent support']*0.45))
        result['request'] = list(answer['antecedents'])

        # Сортируем результат по метрике
        result = result.sort_values(by='metric', ascending=False)

        # Удаляем из результата повторения по id в общих результатах
        try:
            result = result[~result['Id'].isin(results['Id'])]
        except:
            pass

        # Берем верхние строки
        result = result[:math.ceil(25 / request.shape[0])]

        # Добавляем результат по одному элементу в общие результаты
        results = pd.concat([results, result], axis=0)

    results = results.sort_values(by='metric', ascending=False).head(25).sort_values(by='Id')
    return results[['request', 'Id', 'Name']].rename(columns={'request': 'ID в запросе', 'Id': 'ID в рекомендации', 'Name': 'Наименование рекомендуемой детали'})


# Определение списка значений для выпадающего списка
requests = random.sample(list(df['StructureId'].unique()), 15)

# Создание объекта интерфейса Gradio с выпадающим списком
iface = gr.Interface(title='Electricity Design Advisor',
                     description='Интерфейс создан для ознакомления с работой рекомендательной системы. '
                                 'В качестве запроса можно выбрать 1 из случайного списка 15 опор ЛЭП и получить '
                                 'в ответ 25 рекомендуемых деталей',
                     fn=get_results,
                     inputs=gr.Dropdown(requests,
                                        label='Опора',
                                        info='Выберите опору ЛЭП из списка'),
                     outputs=gr.Dataframe(col_count=3,
                                          max_rows=25,
                                          headers=['ID в запросе', 'ID в рекомендации', 'Наименование рекомендуемой детали'],
                                          show_label=True,
                                          wrap=True,
                                          label='Список рекомендуемых деталей к выбранной опоре'),
                     allow_flagging='never',
                     theme=gr.themes.Soft())

if __name__ == "__main__":
    iface.launch()#server_name="95.140.157.138", server_port=7777)
