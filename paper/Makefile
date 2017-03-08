TEXFILES=ILP_formulation3.tex

MAIN=ILP_formulation3

all: $(MAIN).pdf

$(MAIN).pdf: $(MAIN).tex
	pdflatex $(MAIN).tex
	bibtex $(MAIN)
	pdflatex $(MAIN).tex
	pdflatex $(MAIN).tex

clean:
	rm $(MAIN).pdf *.log *.blg *.aux *.out *.bbl
