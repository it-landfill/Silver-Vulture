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
	if not genre_span:
		genre_span = soup.find("span", string="Genre:")
		
	if not genre_span:
		print(f"Anime {anime_id} has no genres")
	else:
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
	l = [[19815, 8.051214652916261],
		[31764, 7.988826085126771],
		[15315, 7.985755468095645],
		[30240, 7.968056867381807],
		[28297,7.9394837790288815],
		[35790, 7.930399889384616],
		[31339, 7.921984325470365],
		[23755, 7.856063230517275],
		[32998, 7.844393835973329],
		[37349,  7.83575235670582],
		[14345, 7.818407260905898],
		[16011, 7.800141698884931],
		[40080, 7.796863898723514],
		[34944,7.7634344138097635],
		[34636, 7.747805252152514],
		[28677, 7.739063186092364],
		[34542, 7.735384600428416],
		[29093, 7.730204624956457],
		[33071,  7.72221360479559],
		[30015, 7.7080932242807]]
	info_list = resolve_anime_list(l)
	for i, anime in enumerate(info_list):
		print(f"{i+1} - {anime}")