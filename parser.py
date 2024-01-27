import asyncio
import pandas as pd
import aiohttp
from bs4 import BeautifulSoup

HEADERS = {'Accept-Language': 'en-US,en;q=0.8', "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"}


class Club:
    def __init__(self, name, link):
        self.name = name
        self.link = link
        self.data = None

    def generate_link(self, year):
        new_link = f"{self.link}/{year}-{year + 1}/"
        return new_link

    async def get_html(self, session, link=None):
        if isinstance(link, str):
            async with session.get(link) as response:
                return await response.text()
        else:
            async with session.get(self.link) as response:
                return await response.text()

    async def parse_report(self, session, link):
        async with (session.get(link, headers=HEADERS) as resp):
            result = await resp.text(errors='replace')
            soup = BeautifulSoup(result, 'lxml')
            goals_home = 0
            goals_away = 0
            goals_home_time = []
            goals_away_time = []
            scorers_home = []
            scorers_away = []
            yellows_home = 0
            yellows_away = 0
            reds_home = 0
            reds_away = 0
            subs_home = 0
            subs_away = 0
            sub_players_home = []
            sub_players_away = []
            subs_home_time = []
            subs_away_time = []
            goals_home_first = []
            goals_home_second = []
            goals_away_first = []
            goals_away_second = []
            own_goals = 0
            penalty = 0
            variants = []

            def time_fix(text) -> int:
                if '45' in text:
                    return 45
                elif '90' in text:
                    return 90
                elif '120' in text:
                    return 120
                else:
                    new_text = text.replace(" ", "")
                    new_text = new_text[:-1]
                    return int(new_text)

            articles = soup.find_all('article', {'class': 'event-container'})
            for article in articles:
                event = article.find('div', {'class': 'event'})
                if event is not None:
                    home_event = event.find('div', {'class': 'team-side--home'})
                    if home_event is not None:
                        home_wrapper = home_event.find('div', {'class': 'team-side__wrapper'})
                        if home_wrapper is not None:
                            home_desc = home_wrapper.find('div', {'class': 'event__description'})
                            div_data = home_desc.find('div')
                            span_data = div_data.findAll('span', {'class': 'event__text'})
                            match span_data[0].text:
                                case "Гол!":
                                    scorer = span_data[1].text
                                    if scorer != '':
                                        time_event = event.find('div', {'class': 'event__match-time-line'})
                                        time = time_event.find('span', {'class': 'time'})
                                        if time is not None:
                                            goals_home += 1
                                            scorers_home.append(scorer)
                                            goals_home_time.append(time_fix(time.text))
                                            try:
                                                if span_data[2].text == " С пенальти ":
                                                    penalty += 1
                                            except IndexError:
                                                pass
                                case "Автогол!":
                                    goals_home += 1
                                    own_goals += 1
                                    scorer = span_data[1].text
                                    scorers_home.append(scorer)
                                    time_event = event.find('div', {'class': 'event__match-time-line'})
                                    time = time_event.find('span', {'class': 'time'})
                                    goals_home_time.append(time_fix(time.text))
                                case "Желтая карточка":
                                    yellows_home += 1
                                case "Красная карточка":
                                    reds_home += 1
                                case "Замена":
                                    subs_home += 1
                                    sub_name = span_data[2].text
                                    sub_players_home.append(sub_name)
                                    time_event = event.find('div', {'class': 'event__match-time-line'})
                                    time = time_event.find('span', {'class': 'time'})
                                    subs_home_time.append(time_fix(time.text))

                    away_event = event.find('div', {'class': 'team-side--away'})
                    if away_event is not None:
                        away_wrapper = away_event.find('div', {'class': 'team-side__wrapper'})
                        if away_wrapper is not None:
                            away_desc = away_wrapper.find('div', {'class': 'event__description'})
                            div_data = away_desc.find('div')
                            span_data = div_data.findAll('span', {'class': 'event__text'})
                            match span_data[0].text:
                                case "Гол!":
                                    scorer = span_data[1].text
                                    if scorer != '':
                                        time_event = event.find('div', {'class': 'event__match-time-line'})
                                        time = time_event.find('span', {'class': 'time'})
                                        if time is not None:
                                            goals_away += 1
                                            scorers_away.append(scorer)
                                            goals_away_time.append(time_fix(time.text))
                                            try:
                                                if span_data[2].text == " С пенальти ":
                                                    penalty += 1
                                            except IndexError:
                                                pass
                                case "Автогол!":
                                    goals_away += 1
                                    own_goals += 1
                                    scorer = span_data[1].text
                                    scorers_away.append(scorer)
                                    time_event = event.find('div', {'class': 'event__match-time-line'})
                                    time = time_event.find('span', {'class': 'time'})
                                    goals_away_time.append(time_fix(time.text))
                                case "Желтая карточка":
                                    yellows_away += 1
                                case "Красная карточка":
                                    reds_away += 1
                                case "Замена":
                                    subs_away += 1
                                    sub_name = span_data[2].text
                                    sub_players_away.append(sub_name)
                                    time_event = event.find('div', {'class': 'event__match-time-line'})
                                    time = time_event.find('span', {'class': 'time'})
                                    subs_away_time.append(time_fix(time.text))

            goals_home_time.reverse()
            scorers_home.reverse()
            goals_away_time.reverse()
            scorers_away.reverse()
            subs_home_time.reverse()
            sub_players_home.reverse()
            subs_away_time.reverse()
            sub_players_away.reverse()

            for i in goals_home_time:
                if i <= 45:
                    goals_home_first.append(i)
                else:
                    goals_home_second.append(i)

            for i in goals_away_time:
                if i <= 45:
                    goals_away_first.append(i)
                else:
                    goals_away_second.append(i)

            if goals_home > goals_away:
                variants.append(1)
            elif goals_home < goals_away:
                variants.append(2)
            else:
                variants.append(3)

            if len(goals_home_first) > len(goals_away_first):
                variants.append(4)
            elif len(goals_home_first) < len(goals_away_first):
                variants.append(5)
            else:
                variants.append(6)

            if len(goals_home_second) > len(goals_away_second):
                variants.append(7)
            elif len(goals_home_second) < len(goals_away_second):
                variants.append(8)
            else:
                variants.append(9)

            if len(goals_home_first) + len(goals_away_first) > len(goals_home_second) + len(goals_away_second):
                variants.append(10)
            elif len(goals_home_first) + len(goals_away_first) < len(goals_home_second) + len(
                    goals_away_second):
                variants.append(11)
            else:
                variants.append(12)

            if goals_home + goals_away == 0:
                variants.append(13)
            elif 1 <= goals_home + goals_away <= 2:
                variants.append(14)
            elif 3 <= goals_home + goals_away <= 4:
                variants.append(15)
            else:
                variants.append(16)

            if goals_away == 0:
                variants.append(17)

            if goals_home == 0:
                variants.append(18)

            if goals_home > goals_away:
                for i in range(len(goals_away_time)):
                    if goals_home_time[i] > goals_away_time[i]:
                        variants.append(19)
                        break
            elif goals_away > goals_home:
                for i in range(len(goals_home_time)):
                    if goals_away_time[i] > goals_home_time[i]:
                        variants.append(20)
                        break
            else:
                for i in range(len(goals_home_time)):
                    if goals_home_time[i] > goals_away_time[i]:
                        variants.append(19)
                        break
                    elif goals_away_time[i] > goals_home_time[i]:
                        variants.append(20)
                        break

            try:
                if 1 <= min(goals_home_time) <= 45 and 1 <= min(goals_away_time) <= 45:
                    variants.append(21)
            except ValueError:
                pass

            try:
                if 46 <= max(goals_home_time) <= 90 and 46 <= max(goals_away_time) <= 90:
                    variants.append(22)
            except ValueError:
                pass

            if len(goals_home_time) > 0 and len(goals_away_time) > 0:
                if min(goals_home_time) < min(goals_away_time):
                    if 1 <= min(goals_home_time) <= 20:
                        variants.append(23)
                    elif 21 <= min(goals_home_time) <= 45:
                        variants.append(24)
                    else:
                        variants.append(25)
                else:
                    if 1 <= min(goals_away_time) <= 20:
                        variants.append(23)
                    elif 21 <= min(goals_away_time) <= 45:
                        variants.append(24)
                    else:
                        variants.append(25)
            elif len(goals_away_time) == 0 and len(goals_home_time) > 0:
                if 1 <= min(goals_home_time) <= 20:
                    variants.append(23)
                elif 21 <= min(goals_home_time) <= 45:
                    variants.append(24)
                else:
                    variants.append(25)
            elif len(goals_home_time) == 0 and len(goals_away_time) > 0:
                if 1 <= min(goals_away_time) <= 20:
                    variants.append(23)
                elif 21 <= min(goals_away_time) <= 45:
                    variants.append(24)
                else:
                    variants.append(25)
            else:
                pass

            if goals_home > goals_away:
                winning_goal = goals_home_time[goals_away]
                if 1 <= winning_goal <= 45:
                    variants.append(26)
                elif 46 <= winning_goal <= 70:
                    variants.append(27)
                elif 71 <= winning_goal <= 90:
                    variants.append(28)
            elif goals_away > goals_home:
                winning_goal = goals_away_time[goals_home]
                if 1 <= winning_goal <= 45:
                    variants.append(26)
                elif 46 <= winning_goal <= 70:
                    variants.append(27)
                elif 71 <= winning_goal <= 90:
                    variants.append(28)

            if goals_home > goals_away:
                temp = goals_home - goals_away
                if temp == 1:
                    variants.append(29)
                elif temp == 2:
                    variants.append(30)
                elif temp >= 3:
                    variants.append(31)
            else:
                temp = goals_away - goals_home
                if temp == 1:
                    variants.append(29)
                elif temp == 2:
                    variants.append(30)
                elif temp >= 3:
                    variants.append(31)

            for scorer in scorers_home:
                if scorers_home.count(scorer) > 1:
                    variants.append(32)
                    break

            for scorer in scorers_away:
                if scorers_away.count(scorer) > 1:
                    if variants.count(32) >= 1:
                        pass
                    else:
                        variants.append(32)
                    break

            yellows = yellows_home + yellows_away
            if yellows <= 3:
                variants.append(33)
            elif 4 <= yellows <= 5:
                variants.append(34)
            else:
                variants.append(35)

            reds = reds_home + reds_away
            if reds > 0 or own_goals > 0:
                variants.append(36)

            if penalty > 0:
                variants.append(37)

            if subs_home <= 4:
                if subs_away <= 4:
                    variants.append(38)

            try:
                if 1 < min(subs_home_time) < 46 or 1 < min(subs_away_time) < 46 or subs_home_time.count(
                        46) == 2 or subs_away_time.count(46) == 2 or (
                        subs_home_time.count(46) == 1 and subs_away_time.count(46) == 1):
                    variants.append(39)
            except ValueError:
                pass

            for sub in sub_players_home:
                if sub in scorers_home:
                    variants.append(40)
                    break

            for sub in sub_players_away:
                if sub in scorers_away:
                    if variants.count(40) > 0:
                        pass
                    else:
                        variants.append(40)
                        break
            home_team = soup.find('span', {'class': 'match-summary__team-name--home'}).find('a')
            if home_team is None:
                home_team = Noname()  # временное решение проблемы игр с командами, для которых нет ссылок
            away_team = soup.find('span', {'class': 'match-summary__team-name--away'}).find('a')
            if away_team is None:
                away_team = Noname()  # временное решение проблемы игр с командами, для которых нет ссылок
            tournament = soup.find('a', {'class': 'top__tournament-name'})
            if tournament is None:
                tournament = soup.find('span', {'class': 'top__tournament-name'})
            if home_team.text == self.name:
                venue = "Дома"
            else:
                venue = "В гостях"

            if venue == "Дома":
                scores_for = goals_home
                scores_against = goals_away
            else:
                scores_for = goals_away
                scores_against = goals_home

            temp_variants = map(str, variants)
            variants_final = "-".join(temp_variants)
            return [tournament.text, home_team.text, away_team.text, goals_home, goals_away,
                    yellows_home + yellows_away, variants_final]

    async def get_reports(self, session, link):
        if isinstance(link, str):
            links = []
            response = await self.get_html(session, link)
            soup = BeautifulSoup(response, 'lxml')
            table = soup.find('table', {'class': 'stat-table'})
            tbody = table.find('tbody')
            for i in tbody:
                if i != "\n" and i is not None:
                    if i.find('td', {'class': 'name-td alLeft'}).find(
                            'div', {'class': 'hide-field'}).a['title'] != "Товарищеские матчи (клубы)" and i.find(
                        'td', {'class': 'name-td alLeft'}).find(
                        'div', {'class': 'hide-field'}).a['title'] != "Товарищеские матчи (сборные)" and i.find(
                        'td', {'class': 'name-td alLeft'}).find(
                        'div', {'class': 'hide-field'}).text != "Россия. Третий дивизион" and i.find(
                        'td', {'class': 'name-td alLeft'}).find(
                        'div', {'class': 'hide-field'}).a['title'] != "Нидерланды. Суперкубок":
                        scoreboard = i.find('td', {'class': 'score-td'})
                        score = scoreboard.find('a')
                        if score.text != "превью" and score.text != "отменен":
                            noindex = score.find('noindex')
                            if noindex is not None:
                                if score['href'][:5] == "https":
                                    links.append(score['href'])
                                else:
                                    links.append("https://www.sports.ru/" + score['href'])
        else:
            links = []
            response = await self.get_html()
            soup = BeautifulSoup(response, 'lxml')
            table = soup.find('table', {'class': 'stat-table'})
            tbody = table.find('tbody')
            for i in tbody:
                if i != "\n" and i is not None:
                    try:
                        if i.find('td', {'class': 'name-td alLeft'}).find(
                                'div', {'class': 'hide-field'}).a['title'] != "Товарищеские матчи (клубы)" and i.find(
                            'td', {'class': 'name-td alLeft'}).find(
                            'div', {'class': 'hide-field'}).a['title'] != "Товарищеские матчи (сборные)" and i.find(
                            'td', {'class': 'name-td alLeft'}).find(
                            'div', {'class': 'hide-field'}).text != "Россия. Третий дивизион" and i.find(
                            'td', {'class': 'name-td alLeft'}).find(
                            'div', {'class': 'hide-field'}).a['title'] != "Нидерланды. Суперкубок":
                            scoreboard = i.find('td', {'class': 'score-td'})
                            score = scoreboard.find('a')
                            if score.text != "превью" and score.text != "отменен":
                                noindex = score.find('noindex')
                                if noindex is not None:
                                    if score['href'][:5] == "https":
                                        links.append(score['href'])
                                    else:
                                        links.append("https://www.sports.ru/" + score['href'])
                    except TypeError:
                        pass
        return links

    async def scrape(self):
        main_link = self.generate_link(2023)
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            tasks = []
            links = await self.get_reports(session, main_link)
            for link in links:
                task = asyncio.create_task(self.parse_report(session, link))
                tasks.append(task)
            data = await asyncio.gather(*tasks)
            self.data = data

    def show(self):
        self.data.reverse()
        end_data = []
        for i in self.data:
            end_data.append([i[0], i[1], i[2], f'{i[3]}:{i[4]}', i[5], i[6]])
        df = pd.DataFrame(end_data, columns=['турнир', 'хозяева', 'гости', 'счёт', 'жёлтые', 'варианты'])
        df_new = df.loc[(df["хозяева"] != "NaN") & (df["гости"] != "NaN")]
        print(df_new.to_string())


class League:
    def __init__(self, name, link):
        self.name = name
        self.link = link

    async def get_html(self, session):
        async with session.get(self.link) as response:
            return await response.text()

    async def get_table(self):
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            response = await self.get_html(session)
            soup = BeautifulSoup(response, 'lxml')
            divs = soup.find_all('div', {'class': 'stat mB6'})
            teams = {}
            if len(divs) == 1:
                selected = divs[0]
                teams_html = selected.find('table', {'class': 'stat-table table sortable-table'}).find('tbody')
                for i in teams_html:
                    if i != "\n":
                        team_a = i.find('td', {'class': 'name-td'}).find('div', {'class': 'hide-field'}).find('a')
                        teams[f'{team_a.text}'] = team_a['href'] + 'calendar/'
            else:
                match self.name:
                    case "Аргентина":
                        selected = divs[-1]
                        teams_html = selected.find('table', {'class': 'stat-table table sortable-table'}).find('tbody')
                        for i in teams_html:
                            if i != "\n":
                                team_a = i.find('td', {'class': 'name-td'}).find('div', {'class': 'hide-field'}).find(
                                    'a')
                                teams[f'{team_a.text}'] = team_a['href'] + 'calendar/'
                    case "МЛС":
                        for j in divs:
                            teams_html = j.find('table', {'class': 'stat-table table sortable-table'}).find('tbody')
                            for i in teams_html:
                                if i != "\n":
                                    team_a = i.find('td', {'class': 'name-td'}).find('div',
                                                                                     {'class': 'hide-field'}).find(
                                        'a')
                                    teams[f'{team_a.text}'] = team_a['href'] + 'calendar/'
                    case _:
                        for j in divs:
                            teams_html = j.find('table', {'class': 'stat-table table sortable-table'}).find('tbody')
                            for i in teams_html:
                                if i != "\n":
                                    team_a = i.find('td', {'class': 'name-td'}).find('div',
                                                                                     {'class': 'hide-field'}).find(
                                        'a')
                                    teams[f'{team_a.text}'] = team_a['href'] + 'calendar/'
        return teams


class Noname:
    def __init__(self):
        self.text = "NaN"


leagues = {
    "АПЛ": 'https://www.sports.ru/epl/table/',
    "Ла Лига": 'https://www.sports.ru/la-liga/table/',
    "Бундеслига": 'https://www.sports.ru/bundesliga/table/',
    "Серия А": 'https://www.sports.ru/seria-a/table/',
    "Лига 1": 'https://www.sports.ru/ligue-1/table/',
    "ФНЛ": 'https://www.sports.ru/1liga/table/',
    "Чемпионшип": 'https://www.sports.ru/championship/table',
    "Нидерланды": 'https://www.sports.ru/eredivisie/table',
    "Бельгия": 'https://www.sports.ru/jupiler-league/table',
    "Португалия": 'https://www.sports.ru/primeira-liga/table',
    "Турция": 'https://www.sports.ru/super-lig/table/',
}
