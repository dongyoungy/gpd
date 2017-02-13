TEXFILES=ILP_formulation.tex

MAIN=ILP_formulation
MAIN2=ILP_formulation2

all: $(MAIN).pdf $(MAIN2).pdf

$(MAIN).pdf: $(MAIN).tex
	pdflatex $(MAIN).tex
	bibtex $(MAIN)
	pdflatex $(MAIN).tex
	pdflatex $(MAIN).tex

$(MAIN2).pdf: $(MAIN2).tex
	pdflatex $(MAIN2).tex
	bibtex $(MAIN2)
	pdflatex $(MAIN2).tex
	pdflatex $(MAIN2).tex

clean:
	rm $(MAIN).pdf $(MAIN2).pdf *.log *.blg *.aux *.out *.bbl
