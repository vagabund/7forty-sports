import os
from parser import *

work_modes = ["Парсинг", "Чтение"]
for mode in work_modes:
    print(work_modes.index(mode) + 1, ".", mode, sep='')
selection = int(input("Выберите режим работы: \n"))
if selection == 1:
    a = list(leagues.keys())
    for i in leagues.keys():
        print(a.index(i) + 1, ".", i, sep='')
    choice_league = int(input("Выберите чемпионат: \n"))
    league_link = leagues.get(a[choice_league - 1])
    league = League(a[choice_league - 1], league_link)
    teams = asyncio.run(league.get_table())
    teams_list = list(teams.keys())
    for i in teams.keys():
        print(teams_list.index(i) + 1, ".", i, sep='')
    choice_team = int(input("Выберите команду: \n"))
    team_link = teams.get(teams_list[choice_team - 1])
    nationals = [17, 18, 19, 20]
    team = Club(teams_list[choice_team - 1], team_link)
    if choice_league in nationals:
        team.national = True
    asyncio.run(team.parse())
    team.show()
    os.system("PAUSE")
elif selection == 2:
    files = os.listdir('./csv')
    for file in files:
        print(files.index(file) + 1, ".", file, sep='')
    selector = int(input("Выберите файл: \n"))
    df = pd.read_csv(f'./csv/{files[selector - 1]}')
    print(df.to_string())
