import requests
from bs4 import BeautifulSoup

class Anime:
	anime_id = 0
	title = ""
	title_english = ""
	avg_score = 0.0
	pred_score = 0.0
	genres = []

	def __init__(self, anime_id):
		self.anime_id = anime_id

	def __str__(self):
		return f"{self.anime_id} - {self.title} - {self.title_english} - {self.pred_score} - {self.avg_score} - {self.genres}"

def resolve_anime_by_id(anime_id):
	URL = f"https://myanimelist.net/anime/{anime_id}"
	page = requests.get(URL)

	soup = BeautifulSoup(page.content, "html.parser")

	a = Anime(anime_id)
	
	# Find title
	title_div = soup.find("div", class_="h1-title")
	if not title_div:
		print(f"Anime {anime_id} not found")
		return None

	title = title_div.find("h1", class_="title-name h1_bold_none")
	if title:
		a.title = title.text
	title_english = title_div.find("p", class_="title-english title-inherit")
	if title_english:
		a.title_english = title_english.text

	# Find score
	score = soup.find("div", class_="score-label")
	if score:
		a.avg_score = float(score.text)

	# Find genres
	genre_span = soup.find("span", string="Genres:")
	genres = [genre.text for genre in genre_span.parent.find_all("a")]
	if genres and len(genres) > 0:
		a.genres = genres
		
	return a

def resolve_anime_list(anime_list):
	info_list = []
	for anime_pred in anime_list:
		anime = resolve_anime_by_id(anime_pred[0])
		if(anime):
			anime.pred_score = anime_pred[1]
			info_list.append(anime)

	return info_list

if __name__ == "__main__":
	l = [[20785, 8.5], [20786, 5.6], [20787, 4.7]]
	info_list = resolve_anime_list(l)
	print(info_list[0])