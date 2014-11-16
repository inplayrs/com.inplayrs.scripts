from lxml import etree    # xml parsing

Team_LeagueIDs = etree.XPath('leagues/league_id')
Team_Name = etree.XPath('name')
Team_Players = etree.XPath('squad/player')
Team_Image = etree.XPath('image')
Player_Image = etree.XPath('image')