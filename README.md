# MGS3 PS2 Tri-Dumped Textures

All .tri textures from all PS2 versions of MGS3, dumps and resturctured for use with mod development with the MGS Master Collection.


## Progress:
All .tri files from every version are presently dumped.

### Master Collection .tri's
	- 100% rebuilt.
	
### Substance: 
	- Europe (Online Beta): 2287 / 2290 Rebuilt (MC Coverage - 100%.)
	- Europe: 9790 / 14407 (MC Coverage - 100%.)
	- Germany: MC Coverage - 100%.
	- Italy: MC Coverage - 100%.
	- Japan: MC Coverage - 100%.
	- Japan (Shokai Seisanban): MC Coverage - 100%.
	- Korea: 9704 / 9734 (MC Coverage - 99%. Only regional magazines/posters & UI left.)
	- Spain: MC Coverage - 100%.
	- USA: 9704 / 9734 (MC Coverage - 99%. Only regional magazines/posters & UI left.) (Self-Note: 1d58c1 = e02_4.bmp_e98df2b1939851f47c8a6c28ea1d4cda)

### Snake Eater:
	- All Versions: 99%. Only regional magazines/posters & UI left.
	
### Trial Edition:
	- 90% done. Camos & early versions of a bunch of early face textures left.
	

-------------

**Question:** 

All these textures appear to be transparent - [#1](https://github.com/dotlessone/MGS3-PS2-Textures/issues/1)

**Answer:** 

That is correct! The PS2 had a different color depth from modern systems, and as a result, fully opaque textures from the PS2 show up as having 50% opacity on PC. 
 - Pixels that have 128 (50%) opacity were actually fully opaque on PS2, pixels that are 102 (40%) opacity were 80% on the PS2, 64 (25%) is 50%, ect.

 - All ports of MGS3 to non-PS2 systems have code that automatically double the opacity level to account for the difference in rendering on other systems. 

  - Stripping opacity outright from a texture / setting it to 100% / fully opaque via photoshop will result in MGS3's lighting engine treating the texture completely different. 

-------------

**Question:** 

What tools are you using for this?

**Answer:**

- Tri Extraction & Rebuilding
  - MGS Tri-Dumper (self-made .tri dumping / rebuilding utility. Will be released publicly once all Snake Eater -> MC mappings have been completed.)

- File Management:
  - Voidtool's Everything

- BP_Asset / Manifest Management:
  - Visual Studio Code
  - Notepad++

- Model Viewers:
  - Jayveer's MGS3 Master Collection & PS2 Noesis plugins (personally modified to fix various corruption issues the original plugin had with ~60% of textures.)

- PCSX2 for .TRI dumped texture verification.
