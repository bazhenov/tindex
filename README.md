# tindex

**Система находится в состоянии технологической демки.**

Система позволяющая индексировать и быстро искать целевые объекты (например, пользователей) по их вхождению в различные множества.

## Что позволяет система

На примере пользователей. Определить группы пользователей и искать кто удовлетворяет целевым критериям. Допустим у нас есть БД пользователей, а также их заходов на сайт

```
# Пользователи зарегистрированные за последние 30 дней
registered_last_30_days :=
    SELECT id
    FROM users
    WHERE register_date > DATE_SUB(TODAY(), INTERVAL 30 DAYS)

# Пользователи заходившие на сайт за последние 7 дней
visits_last_7_days :=
    SELECT user_id
    FROM sessions
    WHERE start_date > DATE_SUB(TODAY(), INTERVAL 1 WEEK)

# Мобильные пользователи
mobile :=
    SELECT user_id
    FROM sessions
    WHERE user_agent = 'mobile'
```

Система по расписанию обновляет идентификаторы пользователей по указанным запросам и сохраняет списки на своей стороне для быстрого поиска. Информация может быть получения из разных не связанных между собой баз данных.

Теперь мы можем узнать какие из мобильных пользователей зарегистрированных в течении последнего месяца не заходили на сайт в течении последней недели:

```
$ tindex query '(registered_last_30_days - visits_last_7_days) & mobile'
```

## Чем это отличается от обыкновенного инвертированного индекса (ИИ)?

Существует масса библиотек позволяющих осуществлять быстрый поиск. Самая популярная, пожалуй, Apache Lucene. Ключевое отличие – tindex реализует "транспонированную индексацию".

### Классическая индексация

Продположим, у нас есть два документа:

```
D1 := w1 w3
D2 := w2 w3
```

Можно представить такой корпус документов в виде таблицы.

|    | w1 | w2 | w3 |
|----|----|----|----|
| D1 |  1 |    |  1 |
| D2 |    |  1 |  1 |

В большинстве систем ИИ индекс обновляется по документам (по строчкам). Если мы хотим обновить документ, мы "вычеркивам" из индекса старую строчку и добавляем новую.

|    | w1 | w2 | w3 |
|----|----|----|----|
| D1 |  1 |    |  1 |
| ~~D2~~ |    |  ~~1~~ |  ~~1~~ |
| D2 |  1  |  1 |   |

Такая индексация требует достаточно сложного процесса оптимизации, чтобы удалять уже неактуальные строчки из индекса.

### Транспонированная индексация

Физически любой инвертированный индекс хранится по колонкам. Именно такой формат хранения обеспечиват основную задачу ИИ – эффективный поиск. Поэтому, индекс можно реализовать гораздо проще если обновлять его по колонкам.

|    | w1 | w2 | ~~w3~~ | w3 |
|----|----|----|----    |--- |
| D1 |  1 |    |  ~~1~~ |  1 |
| D2 |    |  1 |  ~~1~~ |    |
| D2 |  1 |  1 |        |    |

В таком варианте не требуется слияние/оптимизация индекса. Процесс обновления вырождается в единомоментную запись нового состояния колонки в индекс.

Тем не менее, в большинстве приложений такой способ обновления неприемлем, так как требуется сохранить согласованное представление документа в индексе. Но в задаче которую решает эта система согласованность между колонками не требуется. Колонки описывают вхождения объекта в разные множества. Продолжая пример с пользователями: установлено ли у пользователя мобильное приложение, пользуется ли он нашими услугами и т.д.